"""
Data loading functions for F1 ETL pipeline.

This module contains functions to load transformed data into Django models.
All operations are idempotent and use transactions for data integrity.
"""
import logging
from typing import Dict, List, Any, Union
from datetime import datetime

import pandas as pd
from django.db import transaction
from django.utils import timezone

from core.models import (
    Driver,
    Constructor,
    Circuit,
    Race,
    Result,
    Qualifying,
    DriverStanding,
    ConstructorStanding,
    DriverMetrics,
    ConstructorMetrics,
)
from etl.load.bulk_operations import safe_bulk_create, safe_bulk_delete

logger = logging.getLogger(__name__)


def dataframe_to_dicts(data: Union[pd.DataFrame, List[Dict]]) -> List[Dict]:
    """
    Convert DataFrame to list of dictionaries.
    
    Args:
        data: DataFrame or list of dicts
        
    Returns:
        List of dictionaries
    """
    if isinstance(data, pd.DataFrame):
        return data.to_dict(orient='records')
    return data


def upsert_drivers(df: Union[pd.DataFrame, List[Dict]]) -> Dict[str, int]:
    """
    Upsert drivers (update or create).
    
    Uses update_or_create for each driver based on driver_id.
    
    Args:
        df: DataFrame or list of dicts with driver data
        
    Returns:
        Dictionary with 'inserted' and 'updated' counts
    """
    records = dataframe_to_dicts(df)
    
    if not records:
        logger.warning("No drivers to upsert")
        return {"inserted": 0, "updated": 0}
    
    inserted = 0
    updated = 0
    
    try:
        with transaction.atomic():
            for record in records:
                # Extract driver_id as primary key
                driver_id = record.get('driver_id')
                
                if not driver_id:
                    logger.warning(f"Skipping driver record without driver_id: {record}")
                    continue
                
                # Prepare defaults (all fields except PK)
                defaults = {
                    'driver_ref': record.get('driver_ref', ''),
                    'number': record.get('driver_number') or record.get('number'),
                    'code': record.get('driver_code') or record.get('code'),
                    'forename': record.get('driver_forename', '') or record.get('forename', ''),
                    'surname': record.get('driver_surname', '') or record.get('surname', ''),
                    'date_of_birth': record.get('driver_dob') or record.get('date_of_birth'),
                    'nationality': record.get('driver_nationality') or record.get('nationality'),
                    'url': record.get('driver_url', '') or record.get('url', ''),
                }
                
                # Remove None values to avoid overwriting with null
                defaults = {k: v for k, v in defaults.items() if v is not None and v != ''}
                
                obj, created = Driver.objects.update_or_create(
                    driver_id=driver_id,
                    defaults=defaults
                )
                
                if created:
                    inserted += 1
                else:
                    updated += 1
        
        logger.info(f"Upserted {inserted + updated} drivers ({inserted} inserted, {updated} updated)")
        return {"inserted": inserted, "updated": updated}
        
    except Exception as e:
        logger.error(f"Error upserting drivers: {e}", exc_info=True)
        raise


def upsert_constructors(df: Union[pd.DataFrame, List[Dict]]) -> Dict[str, int]:
    """
    Upsert constructors (update or create).
    
    Uses update_or_create for each constructor based on constructor_id.
    
    Args:
        df: DataFrame or list of dicts with constructor data
        
    Returns:
        Dictionary with 'inserted' and 'updated' counts
    """
    records = dataframe_to_dicts(df)
    
    if not records:
        logger.warning("No constructors to upsert")
        return {"inserted": 0, "updated": 0}
    
    inserted = 0
    updated = 0
    
    try:
        with transaction.atomic():
            for record in records:
                constructor_id = record.get('constructor_id')
                
                if not constructor_id:
                    logger.warning(f"Skipping constructor without constructor_id: {record}")
                    continue
                
                defaults = {
                    'constructor_ref': record.get('constructor_ref', ''),
                    'name': record.get('constructor_name', '') or record.get('name', ''),
                    'nationality': record.get('constructor_nationality') or record.get('nationality'),
                    'url': record.get('constructor_url', '') or record.get('url', ''),
                }
                
                # Remove None/empty values
                defaults = {k: v for k, v in defaults.items() if v is not None and v != ''}
                
                obj, created = Constructor.objects.update_or_create(
                    constructor_id=constructor_id,
                    defaults=defaults
                )
                
                if created:
                    inserted += 1
                else:
                    updated += 1
        
        logger.info(f"Upserted {inserted + updated} constructors ({inserted} inserted, {updated} updated)")
        return {"inserted": inserted, "updated": updated}
        
    except Exception as e:
        logger.error(f"Error upserting constructors: {e}", exc_info=True)
        raise


def upsert_circuits(df: Union[pd.DataFrame, List[Dict]]) -> Dict[str, int]:
    """
    Upsert circuits (update or create).
    
    Uses update_or_create for each circuit based on circuit_id.
    
    Args:
        df: DataFrame or list of dicts with circuit data
        
    Returns:
        Dictionary with 'inserted' and 'updated' counts
    """
    records = dataframe_to_dicts(df)
    
    if not records:
        logger.warning("No circuits to upsert")
        return {"inserted": 0, "updated": 0}
    
    inserted = 0
    updated = 0
    
    try:
        with transaction.atomic():
            for record in records:
                circuit_id = record.get('circuit_id')
                
                if not circuit_id:
                    logger.warning(f"Skipping circuit without circuit_id: {record}")
                    continue
                
                defaults = {
                    'circuit_ref': record.get('circuit_ref', ''),
                    'name': record.get('circuit_name', '') or record.get('name', ''),
                    'location': record.get('location', ''),
                    'country': record.get('country', ''),
                    'latitude': record.get('latitude'),
                    'longitude': record.get('longitude'),
                    'altitude': record.get('altitude'),
                    'url': record.get('url', ''),
                }
                
                # Remove None/empty values
                defaults = {k: v for k, v in defaults.items() if v is not None and v != ''}
                
                obj, created = Circuit.objects.update_or_create(
                    circuit_id=circuit_id,
                    defaults=defaults
                )
                
                if created:
                    inserted += 1
                else:
                    updated += 1
        
        logger.info(f"Upserted {inserted + updated} circuits ({inserted} inserted, {updated} updated)")
        return {"inserted": inserted, "updated": updated}
        
    except Exception as e:
        logger.error(f"Error upserting circuits: {e}", exc_info=True)
        raise


def upsert_race(df_single_race: Union[pd.DataFrame, Dict]) -> Dict[str, int]:
    """
    Upsert a single race (update or create).
    
    Uses update_or_create based on (season, round) constraint.
    
    Args:
        df_single_race: DataFrame with single row or dict with race data
        
    Returns:
        Dictionary with 'inserted' or 'updated' count (always 1 or 0)
    """
    if isinstance(df_single_race, pd.DataFrame):
        if len(df_single_race) == 0:
            logger.warning("Empty DataFrame provided for race upsert")
            return {"inserted": 0, "updated": 0}
        record = df_single_race.iloc[0].to_dict()
    else:
        record = df_single_race
    
    try:
        with transaction.atomic():
            season = int(record.get('season'))
            round_number = int(record.get('round'))
            circuit_id = record.get('circuit_id')
            
            # Get or create circuit first
            circuit = Circuit.objects.get(circuit_id=circuit_id)
            
            defaults = {
                'circuit': circuit,
                'race_name': record.get('race_name', ''),
                'race_date': record.get('race_date'),
                'race_time': record.get('race_time'),
                'url': record.get('url', ''),
            }
            
            # Remove None values
            defaults = {k: v for k, v in defaults.items() if v is not None}
            
            race, created = Race.objects.update_or_create(
                season=season,
                round=round_number,
                defaults=defaults
            )
            
            if created:
                logger.info(f"Inserted race: {season} Round {round_number} - {race.race_name}")
                return {"inserted": 1, "updated": 0}
            else:
                logger.info(f"Updated race: {season} Round {round_number} - {race.race_name}")
                return {"inserted": 0, "updated": 1}
                
    except Circuit.DoesNotExist:
        logger.error(f"Circuit {circuit_id} not found for race {season}-{round_number}")
        raise
    except Exception as e:
        logger.error(f"Error upserting race: {e}", exc_info=True)
        raise


def replace_results(race_id: int, df_results: Union[pd.DataFrame, List[Dict]]) -> Dict[str, int]:
    """
    Replace results for a specific race (delete + insert).
    
    Args:
        race_id: Race primary key
        df_results: DataFrame or list of dicts with result data
        
    Returns:
        Dictionary with 'deleted' and 'inserted' counts
    """
    records = dataframe_to_dicts(df_results)
    
    if not records:
        logger.warning(f"No results to load for race {race_id}")
        return {"deleted": 0, "inserted": 0}
    
    try:
        with transaction.atomic():
            # Get race object
            race = Race.objects.get(race_id=race_id)
            
            # Delete existing results
            deleted = safe_bulk_delete(
                Result.objects.filter(race=race),
                model_name="Result"
            )
            
            # Create new result objects
            result_objects = []
            for record in records:
                driver_id = record.get('driver_id')
                constructor_id = record.get('constructor_id')
                
                try:
                    driver = Driver.objects.get(driver_id=driver_id)
                    constructor = Constructor.objects.get(constructor_id=constructor_id)
                    
                    result = Result(
                        race=race,
                        driver=driver,
                        constructor=constructor,
                        number=record.get('number') or 0,
                        grid=int(record.get('grid', 0)),
                        position=record.get('position'),  # nullable
                        position_text=record.get('position_text', ''),
                        position_order=int(record.get('position_order', 0)),
                        points=float(record.get('points', 0)),
                        laps=int(record.get('laps', 0)),
                        time_milliseconds=record.get('time_milliseconds'),
                        fastest_lap=record.get('fastest_lap'),
                        fastest_lap_rank=record.get('fastest_lap_rank'),
                        fastest_lap_time=record.get('fastest_lap_time'),
                        fastest_lap_speed=record.get('fastest_lap_speed'),
                        status=record.get('status', ''),
                    )
                    result_objects.append(result)
                    
                except (Driver.DoesNotExist, Constructor.DoesNotExist) as e:
                    logger.warning(f"Skipping result due to missing FK: {e}")
                    continue
            
            # Bulk insert
            inserted = safe_bulk_create(Result, result_objects)
            
            logger.info(
                f"Replaced results for race {race_id}: "
                f"deleted {deleted}, inserted {inserted}"
            )
            
            return {"deleted": deleted, "inserted": inserted}
            
    except Race.DoesNotExist:
        logger.error(f"Race {race_id} not found")
        raise
    except Exception as e:
        logger.error(f"Error replacing results for race {race_id}: {e}", exc_info=True)
        raise


def replace_qualifying(race_id: int, df_qualifying: Union[pd.DataFrame, List[Dict]]) -> Dict[str, int]:
    """
    Replace qualifying results for a specific race (delete + insert).
    
    Args:
        race_id: Race primary key
        df_qualifying: DataFrame or list of dicts with qualifying data
        
    Returns:
        Dictionary with 'deleted' and 'inserted' counts
    """
    records = dataframe_to_dicts(df_qualifying)
    
    if not records:
        logger.warning(f"No qualifying data to load for race {race_id}")
        return {"deleted": 0, "inserted": 0}
    
    try:
        with transaction.atomic():
            # Get race object
            race = Race.objects.get(race_id=race_id)
            
            # Delete existing qualifying results
            deleted = safe_bulk_delete(
                Qualifying.objects.filter(race=race),
                model_name="Qualifying"
            )
            
            # Create new qualifying objects
            qualifying_objects = []
            for record in records:
                driver_id = record.get('driver_id')
                constructor_id = record.get('constructor_id')
                
                try:
                    driver = Driver.objects.get(driver_id=driver_id)
                    constructor = Constructor.objects.get(constructor_id=constructor_id)
                    
                    qualifying = Qualifying(
                        race=race,
                        driver=driver,
                        constructor=constructor,
                        position=int(record.get('position', 0)),
                        q1_time=record.get('q1_time'),
                        q2_time=record.get('q2_time'),
                        q3_time=record.get('q3_time'),
                    )
                    qualifying_objects.append(qualifying)
                    
                except (Driver.DoesNotExist, Constructor.DoesNotExist) as e:
                    logger.warning(f"Skipping qualifying due to missing FK: {e}")
                    continue
            
            # Bulk insert
            inserted = safe_bulk_create(Qualifying, qualifying_objects)
            
            logger.info(
                f"Replaced qualifying for race {race_id}: "
                f"deleted {deleted}, inserted {inserted}"
            )
            
            return {"deleted": deleted, "inserted": inserted}
            
    except Race.DoesNotExist:
        logger.error(f"Race {race_id} not found")
        raise
    except Exception as e:
        logger.error(f"Error replacing qualifying for race {race_id}: {e}", exc_info=True)
        raise


def replace_driver_standings(race_id: int, df_standings: Union[pd.DataFrame, List[Dict]]) -> Dict[str, int]:
    """
    Replace driver standings for a specific race (delete + insert).
    
    Args:
        race_id: Race primary key
        df_standings: DataFrame or list of dicts with driver standing data
        
    Returns:
        Dictionary with 'deleted' and 'inserted' counts
    """
    records = dataframe_to_dicts(df_standings)
    
    if not records:
        logger.warning(f"No driver standings to load for race {race_id}")
        return {"deleted": 0, "inserted": 0}
    
    try:
        with transaction.atomic():
            # Get race object
            race = Race.objects.get(race_id=race_id)
            
            # Delete existing standings
            deleted = safe_bulk_delete(
                DriverStanding.objects.filter(race=race),
                model_name="DriverStanding"
            )
            
            # Create new standing objects
            standing_objects = []
            for record in records:
                driver_id = record.get('driver_id')
                
                try:
                    driver = Driver.objects.get(driver_id=driver_id)
                    
                    standing = DriverStanding(
                        race=race,
                        driver=driver,
                        points=float(record.get('points', 0)),
                        position=int(record.get('position', 0)),
                        position_text=record.get('position_text', ''),
                        wins=int(record.get('wins', 0)),
                    )
                    standing_objects.append(standing)
                    
                except Driver.DoesNotExist:
                    logger.warning(f"Skipping standing for missing driver: {driver_id}")
                    continue
            
            # Bulk insert
            inserted = safe_bulk_create(DriverStanding, standing_objects)
            
            logger.info(
                f"Replaced driver standings for race {race_id}: "
                f"deleted {deleted}, inserted {inserted}"
            )
            
            return {"deleted": deleted, "inserted": inserted}
            
    except Race.DoesNotExist:
        logger.error(f"Race {race_id} not found")
        raise
    except Exception as e:
        logger.error(f"Error replacing driver standings for race {race_id}: {e}", exc_info=True)
        raise


def replace_constructor_standings(race_id: int, df_standings: Union[pd.DataFrame, List[Dict]]) -> Dict[str, int]:
    """
    Replace constructor standings for a specific race (delete + insert).
    
    Args:
        race_id: Race primary key
        df_standings: DataFrame or list of dicts with constructor standing data
        
    Returns:
        Dictionary with 'deleted' and 'inserted' counts
    """
    records = dataframe_to_dicts(df_standings)
    
    if not records:
        logger.warning(f"No constructor standings to load for race {race_id}")
        return {"deleted": 0, "inserted": 0}
    
    try:
        with transaction.atomic():
            # Get race object
            race = Race.objects.get(race_id=race_id)
            
            # Delete existing standings
            deleted = safe_bulk_delete(
                ConstructorStanding.objects.filter(race=race),
                model_name="ConstructorStanding"
            )
            
            # Create new standing objects
            standing_objects = []
            for record in records:
                constructor_id = record.get('constructor_id')
                
                try:
                    constructor = Constructor.objects.get(constructor_id=constructor_id)
                    
                    standing = ConstructorStanding(
                        race=race,
                        constructor=constructor,
                        points=float(record.get('points', 0)),
                        position=int(record.get('position', 0)),
                        position_text=record.get('position_text', ''),
                        wins=int(record.get('wins', 0)),
                    )
                    standing_objects.append(standing)
                    
                except Constructor.DoesNotExist:
                    logger.warning(f"Skipping standing for missing constructor: {constructor_id}")
                    continue
            
            # Bulk insert
            inserted = safe_bulk_create(ConstructorStanding, standing_objects)
            
            logger.info(
                f"Replaced constructor standings for race {race_id}: "
                f"deleted {deleted}, inserted {inserted}"
            )
            
            return {"deleted": deleted, "inserted": inserted}
            
    except Race.DoesNotExist:
        logger.error(f"Race {race_id} not found")
        raise
    except Exception as e:
        logger.error(f"Error replacing constructor standings for race {race_id}: {e}", exc_info=True)
        raise


def replace_driver_metrics(season: int, df_metrics: Union[pd.DataFrame, List[Dict]]) -> Dict[str, int]:
    """
    Replace driver metrics for a specific season (delete + insert).
    
    Args:
        season: Season year
        df_metrics: DataFrame or list of dicts with driver metrics
        
    Returns:
        Dictionary with 'deleted' and 'inserted' counts
    """
    records = dataframe_to_dicts(df_metrics)
    
    if not records:
        logger.warning(f"No driver metrics to load for season {season}")
        return {"deleted": 0, "inserted": 0}
    
    try:
        with transaction.atomic():
            # Delete existing metrics for this season
            deleted = safe_bulk_delete(
                DriverMetrics.objects.filter(season=season),
                model_name="DriverMetrics"
            )
            
            # Create new metric objects
            metric_objects = []
            for record in records:
                driver_id = record.get('driver_id')
                
                try:
                    driver = Driver.objects.get(driver_id=driver_id)
                    
                    metrics = DriverMetrics(
                        driver=driver,
                        season=int(record.get('season', season)),
                        races_entered=int(record.get('races_entered', 0)),
                        races_finished=int(record.get('races_finished', 0)),
                        podiums=int(record.get('podiums', 0)),
                        wins=int(record.get('wins', 0)),
                        poles=int(record.get('poles', 0)),
                        dnf_count=int(record.get('dnf_count', 0)),
                        avg_finish_position=record.get('avg_finish_position'),
                        avg_grid_position=record.get('avg_grid_position'),
                        avg_points_per_race=float(record.get('avg_points_per_race', 0)),
                        total_points=float(record.get('total_points', 0)),
                        position_changes_sum=int(record.get('position_changes_sum', 0)),
                        consistency_score=float(record.get('consistency_score', 0)),
                    )
                    metric_objects.append(metrics)
                    
                except Driver.DoesNotExist:
                    logger.warning(f"Skipping metrics for missing driver: {driver_id}")
                    continue
            
            # Bulk insert
            inserted = safe_bulk_create(DriverMetrics, metric_objects)
            
            logger.info(
                f"Replaced driver metrics for season {season}: "
                f"deleted {deleted}, inserted {inserted}"
            )
            
            return {"deleted": deleted, "inserted": inserted}
            
    except Exception as e:
        logger.error(f"Error replacing driver metrics for season {season}: {e}", exc_info=True)
        raise


def replace_constructor_metrics(season: int, df_metrics: Union[pd.DataFrame, List[Dict]]) -> Dict[str, int]:
    """
    Replace constructor metrics for a specific season (delete + insert).
    
    Args:
        season: Season year
        df_metrics: DataFrame or list of dicts with constructor metrics
        
    Returns:
        Dictionary with 'deleted' and 'inserted' counts
    """
    records = dataframe_to_dicts(df_metrics)
    
    if not records:
        logger.warning(f"No constructor metrics to load for season {season}")
        return {"deleted": 0, "inserted": 0}
    
    try:
        with transaction.atomic():
            # Delete existing metrics for this season
            deleted = safe_bulk_delete(
                ConstructorMetrics.objects.filter(season=season),
                model_name="ConstructorMetrics"
            )
            
            # Create new metric objects
            metric_objects = []
            for record in records:
                constructor_id = record.get('constructor_id')
                
                try:
                    constructor = Constructor.objects.get(constructor_id=constructor_id)
                    
                    metrics = ConstructorMetrics(
                        constructor=constructor,
                        season=int(record.get('season', season)),
                        races_entered=int(record.get('races_entered', 0)),
                        podiums=int(record.get('podiums', 0)),
                        wins=int(record.get('wins', 0)),
                        one_two_finishes=int(record.get('one_two_finishes', 0)),
                        double_dnf=int(record.get('double_dnf', 0)),
                        avg_finish_position=record.get('avg_finish_position'),
                        total_points=float(record.get('total_points', 0)),
                        reliability_rate=float(record.get('reliability_rate', 0)),
                    )
                    metric_objects.append(metrics)
                    
                except Constructor.DoesNotExist:
                    logger.warning(f"Skipping metrics for missing constructor: {constructor_id}")
                    continue
            
            # Bulk insert
            inserted = safe_bulk_create(ConstructorMetrics, metric_objects)
            
            logger.info(
                f"Replaced constructor metrics for season {season}: "
                f"deleted {deleted}, inserted {inserted}"
            )
            
            return {"deleted": deleted, "inserted": inserted}
            
    except Exception as e:
        logger.error(f"Error replacing constructor metrics for season {season}: {e}", exc_info=True)
        raise