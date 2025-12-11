"""
ETL Pipeline Orchestrator for F1 data.

This module coordinates the Extract, Transform, and Load phases
of the F1 data pipeline.
"""
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

from django.db import transaction
from django.utils import timezone

from core.models import ETLRun, Race
from etl.config import START_SEASON, END_SEASON
from etl.extract.ergast_client import ErgastClient
from etl.extract.extractors import (
    fetch_season_races,
    fetch_race_results,
    fetch_qualifying,
    fetch_driver_standings,
    fetch_constructor_standings,
)
from etl.transform.parsers import (
    parse_races_json,
    parse_results_json,
    parse_qualifying_json,
    parse_driver_standings_json,
    parse_constructor_standings_json,
)
from etl.transform.cleaners import (
    clean_races_df,
    clean_results_df,
    clean_qualifying_df,
    clean_driver_standings_df,
    clean_constructor_standings_df,
)
from etl.transform.calculators import (
    compute_driver_metrics,
    compute_constructor_metrics,
)
from etl.transform.validators import (
    validate_races_df,
    validate_results_df,
    validate_qualifying_df,
    validate_driver_metrics_df,
    validate_constructor_metrics_df,
)
from etl.load.loaders import (
    upsert_drivers,
    upsert_constructors,
    upsert_circuits,
    upsert_race,
    replace_results,
    replace_qualifying,
    replace_driver_standings,
    replace_constructor_standings,
    replace_driver_metrics,
    replace_constructor_metrics,
)

logger = logging.getLogger(__name__)


def determine_seasons_to_process(mode: str, seasons: Optional[List[int]] = None) -> List[int]:
    """
    Determine which seasons to process based on mode and input.
    
    Args:
        mode: Execution mode ('backfill', 'season', 'incremental')
        seasons: Optional list of specific seasons
        
    Returns:
        List of season years to process
        
    Raises:
        ValueError: If mode is invalid or required seasons are missing
    """
    if mode == 'backfill':
        # Process historical range
        if seasons:
            return sorted(seasons)
        else:
            # Use default range from config
            return list(range(START_SEASON, END_SEASON + 1))
    
    elif mode == 'season':
        # Process specific seasons
        if not seasons:
            raise ValueError("Mode 'season' requires --seasons argument")
        return sorted(seasons)
    
    elif mode == 'incremental':
        # Process only current season
        current_season = END_SEASON  # Simplification: use END_SEASON as current
        return [current_season]
    
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'backfill', 'season', or 'incremental'")


def extract_season_data(
    client: ErgastClient,
    season: int,
    save_raw: bool = False
) -> Dict[str, Any]:
    """
    Extract all data for a given season from Ergast API.
    
    Args:
        client: Ergast API client
        season: Season year
        save_raw: Whether to save raw JSON files
        
    Returns:
        Dictionary with extracted data:
            - races_json: Raw JSON from races endpoint
            - results_by_round: Dict mapping round -> results JSON
            - qualifying_by_round: Dict mapping round -> qualifying JSON
            - driver_standings_json: Raw JSON for driver standings
            - constructor_standings_json: Raw JSON for constructor standings
    """
    logger.info(f"=== Extracting data for season {season} ===")
    
    # Fetch races for the season
    races_json = fetch_season_races(client, season, save_raw=save_raw)
    
    # Parse races to get round numbers
    try:
        races = races_json['MRData']['RaceTable']['Races']
        logger.info(f"Found {len(races)} races for season {season}")
    except KeyError:
        logger.warning(f"No races found for season {season}")
        races = []
    
    # Fetch results and qualifying for each round
    results_by_round = {}
    qualifying_by_round = {}
    
    for race in races:
        round_num = int(race['round'])
        
        try:
            # Fetch results
            results_json = fetch_race_results(client, season, round_num, save_raw=save_raw)
            results_by_round[round_num] = results_json
            
            # Fetch qualifying
            qualifying_json = fetch_qualifying(client, season, round_num, save_raw=save_raw)
            qualifying_by_round[round_num] = qualifying_json
            
            logger.info(f"Extracted data for season {season}, round {round_num}")
            
        except Exception as e:
            logger.error(f"Failed to extract data for season {season}, round {round_num}: {e}")
            continue
    
    # Fetch standings
    driver_standings_json = fetch_driver_standings(client, season, save_raw=save_raw)
    constructor_standings_json = fetch_constructor_standings(client, season, save_raw=save_raw)
    
    logger.info(f"Completed extraction for season {season}")
    
    return {
        'season': season,
        'races_json': races_json,
        'results_by_round': results_by_round,
        'qualifying_by_round': qualifying_by_round,
        'driver_standings_json': driver_standings_json,
        'constructor_standings_json': constructor_standings_json,
    }


def transform_season_data(extracted_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform extracted data into clean DataFrames ready for loading.
    
    Args:
        extracted_data: Dictionary with extracted raw data
        
    Returns:
        Dictionary with transformed DataFrames:
            - races_df, results_df, qualifying_df
            - driver_standings_df, constructor_standings_df
            - drivers_df, constructors_df, circuits_df
            - driver_metrics_df, constructor_metrics_df
    """
    season = extracted_data['season']
    logger.info(f"=== Transforming data for season {season} ===")
    
    # Parse races
    races_df = parse_races_json(extracted_data['races_json'])
    races_df = clean_races_df(races_df)
    validate_races_df(races_df)
    
    # Parse and combine results from all rounds
    all_results = []
    all_qualifying = []
    
    for round_num, results_json in extracted_data['results_by_round'].items():
        # Parse results
        results_df = parse_results_json(results_json, season, round_num)
        if not results_df.empty:
            all_results.append(results_df)
        
        # Parse qualifying
        if round_num in extracted_data['qualifying_by_round']:
            qualifying_json = extracted_data['qualifying_by_round'][round_num]
            qualifying_df = parse_qualifying_json(qualifying_json, season, round_num)
            if not qualifying_df.empty:
                all_qualifying.append(qualifying_df)
    
    # Combine all results and qualifying
    import pandas as pd
    
    if all_results:
        combined_results = pd.concat(all_results, ignore_index=True)
        combined_results = clean_results_df(combined_results)
        validate_results_df(combined_results)
    else:
        combined_results = pd.DataFrame()
    
    if all_qualifying:
        combined_qualifying = pd.concat(all_qualifying, ignore_index=True)
        combined_qualifying = clean_qualifying_df(combined_qualifying)
        validate_qualifying_df(combined_qualifying)
    else:
        combined_qualifying = pd.DataFrame()
    
    # Parse standings
    driver_standings_df = parse_driver_standings_json(
        extracted_data['driver_standings_json'],
        season
    )
    driver_standings_df = clean_driver_standings_df(driver_standings_df)
    
    constructor_standings_df = parse_constructor_standings_json(
        extracted_data['constructor_standings_json'],
        season
    )
    constructor_standings_df = clean_constructor_standings_df(constructor_standings_df)
    
    # Extract unique drivers, constructors, and circuits
    drivers_df = extract_unique_entities(combined_results, 'driver')
    constructors_df = extract_unique_entities(combined_results, 'constructor')
    circuits_df = races_df[['circuit_id', 'circuit_ref', 'circuit_name', 'location', 
                             'country', 'latitude', 'longitude', 'url']].drop_duplicates()
    
    # Calculate metrics
    driver_metrics_df = compute_driver_metrics(combined_results, combined_qualifying)
    validate_driver_metrics_df(driver_metrics_df)
    
    constructor_metrics_df = compute_constructor_metrics(combined_results)
    validate_constructor_metrics_df(constructor_metrics_df)
    
    logger.info(f"Completed transformation for season {season}")
    
    return {
        'races_df': races_df,
        'results_df': combined_results,
        'qualifying_df': combined_qualifying,
        'driver_standings_df': driver_standings_df,
        'constructor_standings_df': constructor_standings_df,
        'drivers_df': drivers_df,
        'constructors_df': constructors_df,
        'circuits_df': circuits_df,
        'driver_metrics_df': driver_metrics_df,
        'constructor_metrics_df': constructor_metrics_df,
    }


def extract_unique_entities(results_df, entity_type: str):
    """
    Extract unique driver or constructor entities from results DataFrame.
    
    Args:
        results_df: Results DataFrame
        entity_type: 'driver' or 'constructor'
        
    Returns:
        DataFrame with unique entities
    """
    import pandas as pd
    
    if results_df.empty:
        return pd.DataFrame()
    
    if entity_type == 'driver':
        cols = ['driver_id', 'driver_ref', 'driver_number', 'driver_code',
                'driver_forename', 'driver_surname', 'driver_dob',
                'driver_nationality', 'driver_url']
        # Rename columns to match model
        entity_df = results_df[cols].drop_duplicates(subset=['driver_id'])
        entity_df = entity_df.rename(columns={
            'driver_number': 'number',
            'driver_code': 'code',
            'driver_forename': 'forename',
            'driver_surname': 'surname',
            'driver_dob': 'date_of_birth',
            'driver_nationality': 'nationality',
            'driver_url': 'url',
        })
    elif entity_type == 'constructor':
        cols = ['constructor_id', 'constructor_ref', 'constructor_name',
                'constructor_nationality', 'constructor_url']
        entity_df = results_df[cols].drop_duplicates(subset=['constructor_id'])
        entity_df = entity_df.rename(columns={
            'constructor_name': 'name',
            'constructor_nationality': 'nationality',
            'constructor_url': 'url',
        })
    else:
        return pd.DataFrame()
    
    return entity_df


def load_season_data(transformed_data: Dict[str, Any]) -> Dict[str, int]:
    """
    Load transformed data into database.
    
    Args:
        transformed_data: Dictionary with transformed DataFrames
        
    Returns:
        Dictionary with load statistics
    """
    logger.info("=== Loading data into database ===")
    
    stats = {
        'drivers_inserted': 0,
        'drivers_updated': 0,
        'constructors_inserted': 0,
        'constructors_updated': 0,
        'circuits_inserted': 0,
        'circuits_updated': 0,
        'races_processed': 0,
        'results_inserted': 0,
        'qualifying_inserted': 0,
    }
    
    # Load master entities
    driver_stats = upsert_drivers(transformed_data['drivers_df'])
    stats['drivers_inserted'] = driver_stats['inserted']
    stats['drivers_updated'] = driver_stats['updated']
    
    constructor_stats = upsert_constructors(transformed_data['constructors_df'])
    stats['constructors_inserted'] = constructor_stats['inserted']
    stats['constructors_updated'] = constructor_stats['updated']
    
    circuit_stats = upsert_circuits(transformed_data['circuits_df'])
    stats['circuits_inserted'] = circuit_stats['inserted']
    stats['circuits_updated'] = circuit_stats['updated']
    
    # Load races and related data
    races_df = transformed_data['races_df']
    results_df = transformed_data['results_df']
    qualifying_df = transformed_data['qualifying_df']
    driver_standings_df = transformed_data['driver_standings_df']
    constructor_standings_df = transformed_data['constructor_standings_df']
    
    for _, race_row in races_df.iterrows():
        season = race_row['season']
        round_num = race_row['round']
        
        # Upsert race
        race_stats = upsert_race(race_row.to_dict())
        stats['races_processed'] += 1
        
        # Get race_id from database
        try:
            race = Race.objects.get(season=season, round=round_num)
            race_id = race.race_id
        except Race.DoesNotExist:
            logger.error(f"Race not found after upsert: {season}-{round_num}")
            continue
        
        # Load results for this race
        race_results = results_df[
            (results_df['season'] == season) & (results_df['round'] == round_num)
        ]
        if not race_results.empty:
            result_stats = replace_results(race_id, race_results)
            stats['results_inserted'] += result_stats['inserted']
        
        # Load qualifying for this race
        race_qualifying = qualifying_df[
            (qualifying_df['season'] == season) & (qualifying_df['round'] == round_num)
        ]
        if not race_qualifying.empty:
            qual_stats = replace_qualifying(race_id, race_qualifying)
            stats['qualifying_inserted'] += qual_stats['inserted']
        
        # Load driver standings (if this is the last race)
        race_driver_standings = driver_standings_df[
            driver_standings_df['round'] == round_num
        ]
        if not race_driver_standings.empty:
            replace_driver_standings(race_id, race_driver_standings)
        
        # Load constructor standings (if this is the last race)
        race_constructor_standings = constructor_standings_df[
            constructor_standings_df['round'] == round_num
        ]
        if not race_constructor_standings.empty:
            replace_constructor_standings(race_id, race_constructor_standings)
    
    # Load metrics
    season = transformed_data['races_df']['season'].iloc[0] if not transformed_data['races_df'].empty else None
    
    if season and not transformed_data['driver_metrics_df'].empty:
        replace_driver_metrics(season, transformed_data['driver_metrics_df'])
    
    if season and not transformed_data['constructor_metrics_df'].empty:
        replace_constructor_metrics(season, transformed_data['constructor_metrics_df'])
    
    logger.info("Completed data loading")
    
    return stats


def run_pipeline(
    mode: str,
    seasons: Optional[List[int]] = None,
    save_raw: bool = False,
) -> Dict[str, Any]:
    """
    Execute the complete F1 ETL pipeline.
    
    This function orchestrates the Extract, Transform, and Load phases
    for F1 data from Ergast API into the database.
    
    Args:
        mode: Execution mode - 'backfill', 'season', or 'incremental'
        seasons: Optional list of specific seasons to process
        save_raw: Whether to save raw JSON files to disk
        
    Returns:
        Dictionary with pipeline execution summary:
            - mode: Execution mode used
            - seasons: List of seasons processed
            - total_races_processed: Number of races loaded
            - total_drivers: Number of drivers upserted
            - total_constructors: Number of constructors upserted
            - status: 'SUCCESS', 'FAILED', or 'PARTIAL'
            - duration_seconds: Pipeline execution time
            
    Raises:
        ValueError: If invalid mode or missing required arguments
    """
    start_time = timezone.now()
    
    # Create ETL run record
    etl_run = ETLRun.objects.create(
        started_at=start_time,
        status='RUNNING',
        seasons_processed=[],
        races_added=0,
    )
    
    logger.info(f"=== Starting ETL Pipeline (Run ID: {etl_run.id}) ===")
    logger.info(f"Mode: {mode}")
    
    try:
        # Determine seasons to process
        seasons_to_process = determine_seasons_to_process(mode, seasons)
        logger.info(f"Seasons to process: {seasons_to_process}")
        
        # Initialize client
        client = ErgastClient()
        
        # Track overall statistics
        total_races = 0
        total_drivers = 0
        total_constructors = 0
        processed_seasons = []
        
        # Process each season
        for season in seasons_to_process:
            try:
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing season {season}")
                logger.info(f"{'='*60}\n")
                
                # EXTRACT
                extracted_data = extract_season_data(client, season, save_raw)
                
                # TRANSFORM
                transformed_data = transform_season_data(extracted_data)
                
                # LOAD
                load_stats = load_season_data(transformed_data)
                
                # Update statistics
                total_races += load_stats['races_processed']
                total_drivers += load_stats['drivers_inserted'] + load_stats['drivers_updated']
                total_constructors += load_stats['constructors_inserted'] + load_stats['constructors_updated']
                processed_seasons.append(season)
                
                logger.info(f"Successfully processed season {season}")
                
            except Exception as e:
                logger.error(f"Error processing season {season}: {e}", exc_info=True)
                # Continue with next season
                continue
        
        # Determine final status
        if len(processed_seasons) == len(seasons_to_process):
            final_status = 'SUCCESS'
        elif len(processed_seasons) > 0:
            final_status = 'PARTIAL'
        else:
            final_status = 'FAILED'
        
        # Update ETL run record
        end_time = timezone.now()
        etl_run.finished_at = end_time
        etl_run.status = final_status
        etl_run.seasons_processed = processed_seasons
        etl_run.races_added = total_races
        etl_run.save()
        
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"ETL Pipeline completed: {final_status}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Seasons processed: {processed_seasons}")
        logger.info(f"Races processed: {total_races}")
        logger.info(f"{'='*60}\n")
        
        return {
            'mode': mode,
            'seasons': processed_seasons,
            'total_races_processed': total_races,
            'total_drivers': total_drivers,
            'total_constructors': total_constructors,
            'status': final_status,
            'duration_seconds': duration,
        }
        
    except Exception as e:
        # Handle critical errors
        logger.error(f"Critical error in ETL pipeline: {e}", exc_info=True)
        
        # Update ETL run record
        etl_run.finished_at = timezone.now()
        etl_run.status = 'FAILED'
        etl_run.error_log = str(e)
        etl_run.save()
        
        raise