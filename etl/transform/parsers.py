"""
Parsers for converting Ergast API JSON responses to pandas DataFrames.
"""
import logging
from typing import Dict, Any, List
import pandas as pd

logger = logging.getLogger(__name__)


def parse_races_json(raw_json: Dict[str, Any]) -> pd.DataFrame:
    """
    Parse races JSON from Ergast API into DataFrame.
    
    Expected JSON structure:
        {
            "MRData": {
                "RaceTable": {
                    "Races": [
                        {
                            "season": "2023",
                            "round": "1",
                            "raceName": "Bahrain Grand Prix",
                            "Circuit": {...},
                            "date": "2023-03-05",
                            "time": "15:00:00Z",
                            ...
                        }
                    ]
                }
            }
        }
    
    Args:
        raw_json: Raw JSON response from Ergast API
        
    Returns:
        DataFrame with columns:
            - season (int)
            - round (int)
            - race_name (str)
            - race_date (str)
            - race_time (str, nullable)
            - circuit_id (str)
            - circuit_ref (str)
            - circuit_name (str)
            - location (str)
            - country (str)
            - latitude (float, nullable)
            - longitude (float, nullable)
            - url (str)
    """
    try:
        races = raw_json['MRData']['RaceTable']['Races']
    except KeyError as e:
        logger.error(f"Unexpected JSON structure: missing key {e}")
        return pd.DataFrame()
    
    if not races:
        logger.warning("No races found in JSON response")
        return pd.DataFrame()
    
    races_data = []
    
    for race in races:
        circuit = race.get('Circuit', {})
        
        race_record = {
            'season': int(race.get('season', 0)),
            'round': int(race.get('round', 0)),
            'race_name': race.get('raceName', ''),
            'race_date': race.get('date', ''),
            'race_time': race.get('time'),
            'circuit_id': circuit.get('circuitId', ''),
            'circuit_ref': circuit.get('circuitRef', ''),
            'circuit_name': circuit.get('circuitName', ''),
            'location': circuit.get('Location', {}).get('locality', ''),
            'country': circuit.get('Location', {}).get('country', ''),
            'latitude': circuit.get('Location', {}).get('lat'),
            'longitude': circuit.get('Location', {}).get('long'),
            'url': race.get('url', ''),
        }
        
        races_data.append(race_record)
    
    df = pd.DataFrame(races_data)
    logger.info(f"Parsed {len(df)} races from JSON")
    
    return df


def parse_results_json(
    raw_json: Dict[str, Any],
    season: int,
    round_number: int
) -> pd.DataFrame:
    """
    Parse race results JSON from Ergast API into DataFrame.
    
    Expected JSON structure:
        {
            "MRData": {
                "RaceTable": {
                    "Races": [
                        {
                            "season": "2023",
                            "round": "1",
                            "Results": [
                                {
                                    "number": "1",
                                    "position": "1",
                                    "positionText": "1",
                                    "points": "25",
                                    "Driver": {...},
                                    "Constructor": {...},
                                    "grid": "1",
                                    "laps": "57",
                                    "status": "Finished",
                                    "Time": {"millis": "5434031"},
                                    "FastestLap": {...}
                                }
                            ]
                        }
                    ]
                }
            }
        }
    
    Args:
        raw_json: Raw JSON response from Ergast API
        season: Season year
        round_number: Round number
        
    Returns:
        DataFrame with columns aligned to Result model:
            - season, round, driver_id, constructor_id, number, grid,
              position, position_text, position_order, points, laps,
              time_milliseconds, fastest_lap, fastest_lap_rank,
              fastest_lap_time, fastest_lap_speed, status
    """
    try:
        races = raw_json['MRData']['RaceTable']['Races']
        if not races:
            logger.warning(f"No races found for season {season}, round {round_number}")
            return pd.DataFrame()
        
        results = races[0].get('Results', [])
    except (KeyError, IndexError) as e:
        logger.error(f"Unexpected JSON structure: {e}")
        return pd.DataFrame()
    
    if not results:
        logger.warning(f"No results found for season {season}, round {round_number}")
        return pd.DataFrame()
    
    results_data = []
    
    for result in results:
        driver = result.get('Driver', {})
        constructor = result.get('Constructor', {})
        fastest_lap = result.get('FastestLap', {})
        time_data = result.get('Time', {})
        
        result_record = {
            'season': season,
            'round': round_number,
            'driver_id': driver.get('driverId', ''),
            'driver_ref': driver.get('driverRef', ''),
            'driver_number': driver.get('permanentNumber'),
            'driver_code': driver.get('code'),
            'driver_forename': driver.get('givenName', ''),
            'driver_surname': driver.get('familyName', ''),
            'driver_dob': driver.get('dateOfBirth'),
            'driver_nationality': driver.get('nationality'),
            'driver_url': driver.get('url', ''),
            'constructor_id': constructor.get('constructorId', ''),
            'constructor_ref': constructor.get('constructorRef', ''),
            'constructor_name': constructor.get('name', ''),
            'constructor_nationality': constructor.get('nationality'),
            'constructor_url': constructor.get('url', ''),
            'number': result.get('number'),
            'grid': int(result.get('grid', 0)),
            'position': result.get('position'),
            'position_text': result.get('positionText', ''),
            'position_order': int(result.get('positionOrder', 0)),
            'points': float(result.get('points', 0)),
            'laps': int(result.get('laps', 0)),
            'time_milliseconds': time_data.get('millis'),
            'fastest_lap': fastest_lap.get('lap'),
            'fastest_lap_rank': fastest_lap.get('rank'),
            'fastest_lap_time': fastest_lap.get('Time', {}).get('time'),
            'fastest_lap_speed': fastest_lap.get('AverageSpeed', {}).get('speed'),
            'status': result.get('status', ''),
        }
        
        results_data.append(result_record)
    
    df = pd.DataFrame(results_data)
    logger.info(f"Parsed {len(df)} results for season {season}, round {round_number}")
    
    return df


def parse_qualifying_json(
    raw_json: Dict[str, Any],
    season: int,
    round_number: int
) -> pd.DataFrame:
    """
    Parse qualifying results JSON from Ergast API into DataFrame.
    
    Expected JSON structure:
        {
            "MRData": {
                "RaceTable": {
                    "Races": [
                        {
                            "season": "2023",
                            "round": "1",
                            "QualifyingResults": [
                                {
                                    "number": "1",
                                    "position": "1",
                                    "Driver": {...},
                                    "Constructor": {...},
                                    "Q1": "1:30.123",
                                    "Q2": "1:29.456",
                                    "Q3": "1:28.789"
                                }
                            ]
                        }
                    ]
                }
            }
        }
    
    Args:
        raw_json: Raw JSON response from Ergast API
        season: Season year
        round_number: Round number
        
    Returns:
        DataFrame with columns aligned to Qualifying model:
            - season, round, driver_id, constructor_id, position,
              q1_time, q2_time, q3_time
    """
    try:
        races = raw_json['MRData']['RaceTable']['Races']
        if not races:
            logger.warning(f"No races found for season {season}, round {round_number}")
            return pd.DataFrame()
        
        qualifying_results = races[0].get('QualifyingResults', [])
    except (KeyError, IndexError) as e:
        logger.error(f"Unexpected JSON structure: {e}")
        return pd.DataFrame()
    
    if not qualifying_results:
        logger.warning(f"No qualifying results for season {season}, round {round_number}")
        return pd.DataFrame()
    
    qualifying_data = []
    
    for result in qualifying_results:
        driver = result.get('Driver', {})
        constructor = result.get('Constructor', {})
        
        qualifying_record = {
            'season': season,
            'round': round_number,
            'driver_id': driver.get('driverId', ''),
            'driver_ref': driver.get('driverRef', ''),
            'driver_number': driver.get('permanentNumber'),
            'driver_code': driver.get('code'),
            'driver_forename': driver.get('givenName', ''),
            'driver_surname': driver.get('familyName', ''),
            'driver_dob': driver.get('dateOfBirth'),
            'driver_nationality': driver.get('nationality'),
            'driver_url': driver.get('url', ''),
            'constructor_id': constructor.get('constructorId', ''),
            'constructor_ref': constructor.get('constructorRef', ''),
            'constructor_name': constructor.get('name', ''),
            'constructor_nationality': constructor.get('nationality'),
            'constructor_url': constructor.get('url', ''),
            'position': int(result.get('position', 0)),
            'q1_time': result.get('Q1'),
            'q2_time': result.get('Q2'),
            'q3_time': result.get('Q3'),
        }
        
        qualifying_data.append(qualifying_record)
    
    df = pd.DataFrame(qualifying_data)
    logger.info(f"Parsed {len(df)} qualifying results for season {season}, round {round_number}")
    
    return df


def parse_driver_standings_json(
    raw_json: Dict[str, Any],
    season: int
) -> pd.DataFrame:
    """
    Parse driver standings JSON from Ergast API into DataFrame.
    
    Expected JSON structure:
        {
            "MRData": {
                "StandingsTable": {
                    "StandingsLists": [
                        {
                            "season": "2023",
                            "round": "22",
                            "DriverStandings": [
                                {
                                    "position": "1",
                                    "positionText": "1",
                                    "points": "575",
                                    "wins": "19",
                                    "Driver": {...},
                                    "Constructors": [...]
                                }
                            ]
                        }
                    ]
                }
            }
        }
    
    Args:
        raw_json: Raw JSON response from Ergast API
        season: Season year
        
    Returns:
        DataFrame with columns aligned to DriverStanding model:
            - season, round, driver_id, points, position, position_text, wins
    """
    try:
        standings_lists = raw_json['MRData']['StandingsTable']['StandingsLists']
        if not standings_lists:
            logger.warning(f"No standings found for season {season}")
            return pd.DataFrame()
        
        standings_list = standings_lists[0]
        round_number = int(standings_list.get('round', 0))
        driver_standings = standings_list.get('DriverStandings', [])
    except (KeyError, IndexError) as e:
        logger.error(f"Unexpected JSON structure: {e}")
        return pd.DataFrame()
    
    if not driver_standings:
        logger.warning(f"No driver standings for season {season}")
        return pd.DataFrame()
    
    standings_data = []
    
    for standing in driver_standings:
        driver = standing.get('Driver', {})
        
        standing_record = {
            'season': season,
            'round': round_number,
            'driver_id': driver.get('driverId', ''),
            'driver_ref': driver.get('driverRef', ''),
            'driver_number': driver.get('permanentNumber'),
            'driver_code': driver.get('code'),
            'driver_forename': driver.get('givenName', ''),
            'driver_surname': driver.get('familyName', ''),
            'driver_dob': driver.get('dateOfBirth'),
            'driver_nationality': driver.get('nationality'),
            'driver_url': driver.get('url', ''),
            'position': int(standing.get('position', 0)),
            'position_text': standing.get('positionText', ''),
            'points': float(standing.get('points', 0)),
            'wins': int(standing.get('wins', 0)),
        }
        
        standings_data.append(standing_record)
    
    df = pd.DataFrame(standings_data)
    logger.info(f"Parsed {len(df)} driver standings for season {season}")
    
    return df


def parse_constructor_standings_json(
    raw_json: Dict[str, Any],
    season: int
) -> pd.DataFrame:
    """
    Parse constructor standings JSON from Ergast API into DataFrame.
    
    Expected JSON structure:
        {
            "MRData": {
                "StandingsTable": {
                    "StandingsLists": [
                        {
                            "season": "2023",
                            "round": "22",
                            "ConstructorStandings": [
                                {
                                    "position": "1",
                                    "positionText": "1",
                                    "points": "860",
                                    "wins": "21",
                                    "Constructor": {...}
                                }
                            ]
                        }
                    ]
                }
            }
        }
    
    Args:
        raw_json: Raw JSON response from Ergast API
        season: Season year
        
    Returns:
        DataFrame with columns aligned to ConstructorStanding model:
            - season, round, constructor_id, points, position, position_text, wins
    """
    try:
        standings_lists = raw_json['MRData']['StandingsTable']['StandingsLists']
        if not standings_lists:
            logger.warning(f"No standings found for season {season}")
            return pd.DataFrame()
        
        standings_list = standings_lists[0]
        round_number = int(standings_list.get('round', 0))
        constructor_standings = standings_list.get('ConstructorStandings', [])
    except (KeyError, IndexError) as e:
        logger.error(f"Unexpected JSON structure: {e}")
        return pd.DataFrame()
    
    if not constructor_standings:
        logger.warning(f"No constructor standings for season {season}")
        return pd.DataFrame()
    
    standings_data = []
    
    for standing in constructor_standings:
        constructor = standing.get('Constructor', {})
        
        standing_record = {
            'season': season,
            'round': round_number,
            'constructor_id': constructor.get('constructorId', ''),
            'constructor_ref': constructor.get('constructorRef', ''),
            'constructor_name': constructor.get('name', ''),
            'constructor_nationality': constructor.get('nationality'),
            'constructor_url': constructor.get('url', ''),
            'position': int(standing.get('position', 0)),
            'position_text': standing.get('positionText', ''),
            'points': float(standing.get('points', 0)),
            'wins': int(standing.get('wins', 0)),
        }
        
        standings_data.append(standing_record)
    
    df = pd.DataFrame(standings_data)
    logger.info(f"Parsed {len(df)} constructor standings for season {season}")
    
    return df