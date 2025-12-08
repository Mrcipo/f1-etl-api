"""
High-level extraction functions for F1 data from Ergast API.
"""
import logging
import os
from typing import Dict, Optional

from etl.config import RAW_DATA_DIR
from etl.extract.ergast_client import ErgastClient
from etl.extract.utils import save_json_to_file

logger = logging.getLogger(__name__)


def fetch_season_races(
    client: ErgastClient,
    season: int,
    save_raw: bool = True
) -> Dict:
    """
    Fetch all races for a given season.
    
    Args:
        client: Ergast API client instance
        season: Year of the season (e.g., 2023)
        save_raw: Whether to save raw JSON to disk
        
    Returns:
        Complete JSON response with race data
    """
    logger.info(f"Fetching races for season {season}")
    
    path = f"/{season}"
    params = {"limit": 100}  # Ensure we get all races
    
    data = client.get(path, params=params)
    
    if save_raw:
        filepath = os.path.join(RAW_DATA_DIR, str(season), "races.json")
        save_json_to_file(data, filepath)
    
    return data


def fetch_race_results(
    client: ErgastClient,
    season: int,
    round_number: int,
    save_raw: bool = True
) -> Dict:
    """
    Fetch race results for a specific race.
    
    Args:
        client: Ergast API client instance
        season: Year of the season
        round_number: Round number within the season
        save_raw: Whether to save raw JSON to disk
        
    Returns:
        Complete JSON response with race results
    """
    logger.info(f"Fetching results for season {season}, round {round_number}")
    
    path = f"/{season}/{round_number}/results"
    params = {"limit": 100}  # Ensure we get all results
    
    data = client.get(path, params=params)
    
    if save_raw:
        filepath = os.path.join(
            RAW_DATA_DIR,
            str(season),
            str(round_number),
            "results.json"
        )
        save_json_to_file(data, filepath)
    
    return data


def fetch_qualifying(
    client: ErgastClient,
    season: int,
    round_number: int,
    save_raw: bool = True
) -> Dict:
    """
    Fetch qualifying results for a specific race.
    
    Args:
        client: Ergast API client instance
        season: Year of the season
        round_number: Round number within the season
        save_raw: Whether to save raw JSON to disk
        
    Returns:
        Complete JSON response with qualifying data
    """
    logger.info(f"Fetching qualifying for season {season}, round {round_number}")
    
    path = f"/{season}/{round_number}/qualifying"
    params = {"limit": 100}  # Ensure we get all qualifying results
    
    data = client.get(path, params=params)
    
    if save_raw:
        filepath = os.path.join(
            RAW_DATA_DIR,
            str(season),
            str(round_number),
            "qualifying.json"
        )
        save_json_to_file(data, filepath)
    
    return data


def fetch_driver_standings(
    client: ErgastClient,
    season: int,
    round_number: Optional[int] = None,
    save_raw: bool = True
) -> Dict:
    """
    Fetch driver championship standings for a season.
    
    Args:
        client: Ergast API client instance
        season: Year of the season
        round_number: Optional specific round (None for final standings)
        save_raw: Whether to save raw JSON to disk
        
    Returns:
        Complete JSON response with driver standings
    """
    if round_number:
        logger.info(
            f"Fetching driver standings for season {season}, round {round_number}"
        )
        path = f"/{season}/{round_number}/driverStandings"
    else:
        logger.info(f"Fetching final driver standings for season {season}")
        path = f"/{season}/driverStandings"
    
    params = {"limit": 100}  # Ensure we get all drivers
    
    data = client.get(path, params=params)
    
    if save_raw:
        if round_number:
            filepath = os.path.join(
                RAW_DATA_DIR,
                str(season),
                str(round_number),
                "driver_standings.json"
            )
        else:
            filepath = os.path.join(
                RAW_DATA_DIR,
                str(season),
                "driver_standings.json"
            )
        save_json_to_file(data, filepath)
    
    return data


def fetch_constructor_standings(
    client: ErgastClient,
    season: int,
    round_number: Optional[int] = None,
    save_raw: bool = True
) -> Dict:
    """
    Fetch constructor championship standings for a season.
    
    Args:
        client: Ergast API client instance
        season: Year of the season
        round_number: Optional specific round (None for final standings)
        save_raw: Whether to save raw JSON to disk
        
    Returns:
        Complete JSON response with constructor standings
    """
    if round_number:
        logger.info(
            f"Fetching constructor standings for season {season}, round {round_number}"
        )
        path = f"/{season}/{round_number}/constructorStandings"
    else:
        logger.info(f"Fetching final constructor standings for season {season}")
        path = f"/{season}/constructorStandings"
    
    params = {"limit": 100}  # Ensure we get all constructors
    
    data = client.get(path, params=params)
    
    if save_raw:
        if round_number:
            filepath = os.path.join(
                RAW_DATA_DIR,
                str(season),
                str(round_number),
                "constructor_standings.json"
            )
        else:
            filepath = os.path.join(
                RAW_DATA_DIR,
                str(season),
                "constructor_standings.json"
            )
        save_json_to_file(data, filepath)
    
    return data


def extract_season_data(
    client: ErgastClient,
    season: int,
    save_raw: bool = True
) -> Dict[str, any]:
    """
    Extract all data for a complete season (races, results, standings).
    
    This is a convenience function that fetches all relevant data for a season
    and returns it in a structured format.
    
    Args:
        client: Ergast API client instance
        season: Year of the season
        save_raw: Whether to save raw JSON files to disk
        
    Returns:
        Dictionary containing:
            - races: List of races
            - results: Dict mapping round -> results
            - qualifying: Dict mapping round -> qualifying
            - driver_standings: Final driver standings
            - constructor_standings: Final constructor standings
    """
    logger.info(f"Extracting complete data for season {season}")
    
    # Fetch season races first
    races_data = fetch_season_races(client, season, save_raw=save_raw)
    
    # Extract race list from response
    try:
        races = races_data['MRData']['RaceTable']['Races']
        logger.info(f"Found {len(races)} races for season {season}")
    except KeyError as e:
        logger.error(f"Unexpected API response structure: {e}")
        races = []
    
    # Fetch data for each race
    results_by_round = {}
    qualifying_by_round = {}
    
    for race in races:
        round_number = int(race['round'])
        
        try:
            # Fetch results
            results_data = fetch_race_results(
                client, season, round_number, save_raw=save_raw
            )
            results_by_round[round_number] = results_data
            
            # Fetch qualifying
            qualifying_data = fetch_qualifying(
                client, season, round_number, save_raw=save_raw
            )
            qualifying_by_round[round_number] = qualifying_data
            
        except Exception as e:
            logger.error(
                f"Failed to fetch data for season {season}, round {round_number}: {e}"
            )
            continue
    
    # Fetch final standings
    driver_standings = fetch_driver_standings(client, season, save_raw=save_raw)
    constructor_standings = fetch_constructor_standings(client, season, save_raw=save_raw)
    
    logger.info(f"Completed extraction for season {season}")
    
    return {
        "season": season,
        "races": races,
        "results": results_by_round,
        "qualifying": qualifying_by_round,
        "driver_standings": driver_standings,
        "constructor_standings": constructor_standings,
    }