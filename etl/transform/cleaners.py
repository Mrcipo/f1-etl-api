"""
Data cleaning functions for F1 ETL pipeline.
"""
import logging
from typing import Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def clean_races_df(races_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean races DataFrame.
    
    Transformations:
        - Convert race_date to datetime
        - Convert race_time to time format (handle nullable)
        - Convert latitude/longitude to float
        - Ensure season and round are integers
    
    Args:
        races_df: Raw races DataFrame from parser
        
    Returns:
        Cleaned DataFrame
    """
    if races_df.empty:
        logger.warning("Empty races DataFrame provided for cleaning")
        return races_df
    
    df = races_df.copy()
    
    # Convert date
    df['race_date'] = pd.to_datetime(df['race_date'], errors='coerce')
    
    # Convert time (nullable)
    if 'race_time' in df.columns:
        # Remove 'Z' suffix if present
        df['race_time'] = df['race_time'].str.replace('Z', '', regex=False)
        # Keep as string for now (Django TimeField can handle it)
    
    # Convert numeric fields
    df['season'] = df['season'].astype(int)
    df['round'] = df['round'].astype(int)
    
    # Convert coordinates to float (nullable)
    if 'latitude' in df.columns:
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    if 'longitude' in df.columns:
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    
    # Ensure string fields are strings
    string_fields = ['race_name', 'circuit_id', 'circuit_ref', 'circuit_name', 
                     'location', 'country', 'url']
    for field in string_fields:
        if field in df.columns:
            df[field] = df[field].astype(str)
    
    logger.info(f"Cleaned {len(df)} races")
    
    return df


def clean_results_df(results_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean results DataFrame.
    
    Transformations:
        - Convert position from string to int (handle "R", "D", "W" as None)
        - Convert numeric fields to appropriate types
        - Handle nullable fields (time_milliseconds, fastest_lap_*)
        - Calculate position_change = grid - position
        - Normalize column names to snake_case
    
    Args:
        results_df: Raw results DataFrame from parser
        
    Returns:
        Cleaned DataFrame with position_change column added
    """
    if results_df.empty:
        logger.warning("Empty results DataFrame provided for cleaning")
        return results_df
    
    df = results_df.copy()
    
    # Ensure season and round are integers
    df['season'] = df['season'].astype(int)
    df['round'] = df['round'].astype(int)
    
    # Convert position to int (handle special cases)
    df['position'] = pd.to_numeric(df['position'], errors='coerce').astype('Int64')
    
    # Ensure position_text is string
    df['position_text'] = df['position_text'].astype(str)
    
    # Convert numeric fields
    df['position_order'] = df['position_order'].astype(int)
    df['grid'] = df['grid'].astype(int)
    df['points'] = df['points'].astype(float)
    df['laps'] = df['laps'].astype(int)
    
    # Convert number to int (nullable for old seasons)
    df['number'] = pd.to_numeric(df['number'], errors='coerce').astype('Int64')
    
    # Convert time_milliseconds (nullable)
    df['time_milliseconds'] = pd.to_numeric(df['time_milliseconds'], errors='coerce').astype('Int64')
    
    # Convert fastest lap fields (all nullable)
    df['fastest_lap'] = pd.to_numeric(df['fastest_lap'], errors='coerce').astype('Int64')
    df['fastest_lap_rank'] = pd.to_numeric(df['fastest_lap_rank'], errors='coerce').astype('Int64')
    df['fastest_lap_speed'] = pd.to_numeric(df['fastest_lap_speed'], errors='coerce')
    
    # fastest_lap_time remains as string (nullable)
    
    # Calculate position_change (only when position is not null)
    df['position_change'] = df.apply(
        lambda row: row['grid'] - row['position'] if pd.notna(row['position']) else None,
        axis=1
    )
    
    # Ensure string fields
    string_fields = ['driver_id', 'constructor_id', 'status', 'position_text']
    for field in string_fields:
        if field in df.columns:
            df[field] = df[field].astype(str)
    
    logger.info(f"Cleaned {len(df)} results")
    
    return df


def clean_qualifying_df(qualifying_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean qualifying DataFrame.
    
    Transformations:
        - Ensure position is integer
        - Keep time fields as strings (q1_time, q2_time, q3_time are nullable)
        - Normalize column names
    
    Args:
        qualifying_df: Raw qualifying DataFrame from parser
        
    Returns:
        Cleaned DataFrame
    """
    if qualifying_df.empty:
        logger.warning("Empty qualifying DataFrame provided for cleaning")
        return qualifying_df
    
    df = qualifying_df.copy()
    
    # Ensure season and round are integers
    df['season'] = df['season'].astype(int)
    df['round'] = df['round'].astype(int)
    
    # Convert position to int
    df['position'] = df['position'].astype(int)
    
    # Ensure string fields
    string_fields = ['driver_id', 'constructor_id']
    for field in string_fields:
        if field in df.columns:
            df[field] = df[field].astype(str)
    
    # Time fields remain as strings (nullable)
    # q1_time, q2_time, q3_time
    
    logger.info(f"Cleaned {len(df)} qualifying results")
    
    return df


def clean_driver_standings_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean driver standings DataFrame.
    
    Transformations:
        - Ensure numeric fields are correct types
        - Normalize column names
    
    Args:
        df: Raw driver standings DataFrame from parser
        
    Returns:
        Cleaned DataFrame
    """
    if df.empty:
        logger.warning("Empty driver standings DataFrame provided for cleaning")
        return df
    
    cleaned = df.copy()
    
    # Ensure numeric fields
    cleaned['season'] = cleaned['season'].astype(int)
    cleaned['round'] = cleaned['round'].astype(int)
    cleaned['position'] = cleaned['position'].astype(int)
    cleaned['points'] = cleaned['points'].astype(float)
    cleaned['wins'] = cleaned['wins'].astype(int)
    
    # Ensure string fields
    cleaned['driver_id'] = cleaned['driver_id'].astype(str)
    cleaned['position_text'] = cleaned['position_text'].astype(str)
    
    logger.info(f"Cleaned {len(cleaned)} driver standings")
    
    return cleaned


def clean_constructor_standings_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean constructor standings DataFrame.
    
    Transformations:
        - Ensure numeric fields are correct types
        - Normalize column names
    
    Args:
        df: Raw constructor standings DataFrame from parser
        
    Returns:
        Cleaned DataFrame
    """
    if df.empty:
        logger.warning("Empty constructor standings DataFrame provided for cleaning")
        return df
    
    cleaned = df.copy()
    
    # Ensure numeric fields
    cleaned['season'] = cleaned['season'].astype(int)
    cleaned['round'] = cleaned['round'].astype(int)
    cleaned['position'] = cleaned['position'].astype(int)
    cleaned['points'] = cleaned['points'].astype(float)
    cleaned['wins'] = cleaned['wins'].astype(int)
    
    # Ensure string fields
    cleaned['constructor_id'] = cleaned['constructor_id'].astype(str)
    cleaned['position_text'] = cleaned['position_text'].astype(str)
    
    logger.info(f"Cleaned {len(cleaned)} constructor standings")
    
    return cleaned