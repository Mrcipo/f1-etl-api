"""
Data validation functions for F1 ETL pipeline.
"""
import logging
from typing import List, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class DataValidationError(Exception):
    """Custom exception for data validation errors."""
    pass


def validate_races_df(races_df: pd.DataFrame) -> None:
    """
    Validate races DataFrame.
    
    Checks:
        - Required fields are not null
        - Season and round are positive integers
        - No duplicate (season, round) combinations
        - Dates are valid
    
    Args:
        races_df: Races DataFrame to validate
        
    Raises:
        DataValidationError: If validation fails
    """
    if races_df.empty:
        logger.warning("Empty races DataFrame provided for validation")
        return
    
    errors = []
    
    # Check required fields
    required_fields = ['season', 'round', 'race_name', 'race_date', 'circuit_id']
    for field in required_fields:
        if field not in races_df.columns:
            errors.append(f"Missing required field: {field}")
        elif races_df[field].isna().any():
            null_count = races_df[field].isna().sum()
            errors.append(f"Field '{field}' has {null_count} null values")
    
    # Check season values are reasonable
    if 'season' in races_df.columns:
        if (races_df['season'] < 1950).any():
            invalid_count = (races_df['season'] < 1950).sum()
            errors.append(f"Found {invalid_count} season values < 1950")
        if (races_df['season'] > 2030).any():
            invalid_count = (races_df['season'] > 2030).sum()
            logger.warning(f"Found {invalid_count} season values > 2030")
    
    # Check round values are positive
    if 'round' in races_df.columns:
        if (races_df['round'] < 1).any():
            invalid_count = (races_df['round'] < 1).sum()
            errors.append(f"Found {invalid_count} round values < 1")
    
    # Check for duplicates
    if 'season' in races_df.columns and 'round' in races_df.columns:
        duplicates = races_df.duplicated(subset=['season', 'round'], keep=False)
        if duplicates.any():
            dup_count = duplicates.sum()
            errors.append(f"Found {dup_count} duplicate (season, round) combinations")
            logger.error(f"Duplicate races:\n{races_df[duplicates][['season', 'round', 'race_name']]}")
    
    # Raise if errors found
    if errors:
        for error in errors:
            logger.error(f"Races validation error: {error}")
        raise DataValidationError(f"Races validation failed with {len(errors)} errors")
    
    logger.info(f"Validated {len(races_df)} races successfully")


def validate_results_df(results_df: pd.DataFrame) -> None:
    """
    Validate results DataFrame.
    
    Checks:
        - Required fields are not null
        - Positions are in valid range (1-30) when not null
        - Points are non-negative
        - No duplicate (season, round, driver_id) combinations
        - Grid positions are valid
        - Laps are non-negative
    
    Args:
        results_df: Results DataFrame to validate
        
    Raises:
        DataValidationError: If critical validation fails
    """
    if results_df.empty:
        logger.warning("Empty results DataFrame provided for validation")
        return
    
    errors = []
    warnings = []
    
    # Check required fields
    required_fields = ['season', 'round', 'driver_id', 'constructor_id', 
                       'grid', 'position_text', 'points', 'laps']
    for field in required_fields:
        if field not in results_df.columns:
            errors.append(f"Missing required field: {field}")
        elif field in ['driver_id', 'constructor_id', 'position_text']:
            # String fields should not be empty
            if results_df[field].isna().any() or (results_df[field] == '').any():
                null_count = (results_df[field].isna() | (results_df[field] == '')).sum()
                errors.append(f"Field '{field}' has {null_count} null/empty values")
    
    # Validate positions (when not null)
    if 'position' in results_df.columns:
        valid_positions = results_df['position'].notna()
        if valid_positions.any():
            out_of_range = (
                (results_df.loc[valid_positions, 'position'] < 1) |
                (results_df.loc[valid_positions, 'position'] > 30)
            )
            if out_of_range.any():
                warnings.append(
                    f"Found {out_of_range.sum()} positions outside range 1-30"
                )
    
    # Validate points are non-negative
    if 'points' in results_df.columns:
        if (results_df['points'] < 0).any():
            negative_count = (results_df['points'] < 0).sum()
            errors.append(f"Found {negative_count} negative point values")
    
    # Validate grid positions
    if 'grid' in results_df.columns:
        if (results_df['grid'] < 0).any():
            negative_grid = (results_df['grid'] < 0).sum()
            warnings.append(f"Found {negative_grid} negative grid positions")
        if (results_df['grid'] > 30).any():
            high_grid = (results_df['grid'] > 30).sum()
            warnings.append(f"Found {high_grid} grid positions > 30")
    
    # Validate laps are non-negative
    if 'laps' in results_df.columns:
        if (results_df['laps'] < 0).any():
            negative_laps = (results_df['laps'] < 0).sum()
            errors.append(f"Found {negative_laps} negative lap values")
    
    # Check for duplicate results
    if all(f in results_df.columns for f in ['season', 'round', 'driver_id']):
        duplicates = results_df.duplicated(
            subset=['season', 'round', 'driver_id'],
            keep=False
        )
        if duplicates.any():
            dup_count = duplicates.sum()
            errors.append(
                f"Found {dup_count} duplicate (season, round, driver) combinations"
            )
            logger.error(
                f"Duplicate results:\n{results_df[duplicates][['season', 'round', 'driver_id', 'position']]}"
            )
    
    # Log warnings (non-critical)
    for warning in warnings:
        logger.warning(f"Results validation warning: {warning}")
    
    # Raise errors if any critical issues
    if errors:
        for error in errors:
            logger.error(f"Results validation error: {error}")
        raise DataValidationError(f"Results validation failed with {len(errors)} errors")
    
    logger.info(f"Validated {len(results_df)} results successfully")


def validate_qualifying_df(qualifying_df: pd.DataFrame) -> None:
    """
    Validate qualifying DataFrame.
    
    Checks:
        - Required fields are not null
        - Positions are valid (1-30)
        - No duplicate (season, round, driver_id) combinations
    
    Args:
        qualifying_df: Qualifying DataFrame to validate
        
    Raises:
        DataValidationError: If validation fails
    """
    if qualifying_df.empty:
        logger.warning("Empty qualifying DataFrame provided for validation")
        return
    
    errors = []
    
    # Check required fields
    required_fields = ['season', 'round', 'driver_id', 'constructor_id', 'position']
    for field in required_fields:
        if field not in qualifying_df.columns:
            errors.append(f"Missing required field: {field}")
        elif qualifying_df[field].isna().any():
            null_count = qualifying_df[field].isna().sum()
            errors.append(f"Field '{field}' has {null_count} null values")
    
    # Validate positions are in reasonable range
    if 'position' in qualifying_df.columns:
        if (qualifying_df['position'] < 1).any():
            invalid_count = (qualifying_df['position'] < 1).sum()
            errors.append(f"Found {invalid_count} positions < 1")
        if (qualifying_df['position'] > 30).any():
            invalid_count = (qualifying_df['position'] > 30).sum()
            logger.warning(f"Found {invalid_count} positions > 30")
    
    # Check for duplicates
    if all(f in qualifying_df.columns for f in ['season', 'round', 'driver_id']):
        duplicates = qualifying_df.duplicated(
            subset=['season', 'round', 'driver_id'],
            keep=False
        )
        if duplicates.any():
            dup_count = duplicates.sum()
            errors.append(
                f"Found {dup_count} duplicate (season, round, driver) combinations"
            )
            logger.error(
                f"Duplicate qualifying:\n{qualifying_df[duplicates][['season', 'round', 'driver_id', 'position']]}"
            )
    
    if errors:
        for error in errors:
            logger.error(f"Qualifying validation error: {error}")
        raise DataValidationError(f"Qualifying validation failed with {len(errors)} errors")
    
    logger.info(f"Validated {len(qualifying_df)} qualifying results successfully")


def validate_driver_metrics_df(df: pd.DataFrame) -> None:
    """
    Validate driver metrics DataFrame.
    
    Checks:
        - Required fields are not null
        - Numeric fields are non-negative where appropriate
        - No duplicate (driver_id, season) combinations
        - Logical consistency (wins <= podiums <= races_finished <= races_entered)
    
    Args:
        df: Driver metrics DataFrame to validate
        
    Raises:
        DataValidationError: If validation fails
    """
    if df.empty:
        logger.warning("Empty driver metrics DataFrame provided for validation")
        return
    
    errors = []
    warnings = []
    
    # Check required fields
    required_fields = ['driver_id', 'season', 'races_entered', 'total_points']
    for field in required_fields:
        if field not in df.columns:
            errors.append(f"Missing required field: {field}")
        elif df[field].isna().any():
            null_count = df[field].isna().sum()
            errors.append(f"Field '{field}' has {null_count} null values")
    
    # Check non-negative fields
    non_negative_fields = ['races_entered', 'races_finished', 'podiums', 'wins', 
                           'poles', 'dnf_count', 'total_points', 'avg_points_per_race']
    for field in non_negative_fields:
        if field in df.columns:
            if (df[field] < 0).any():
                negative_count = (df[field] < 0).sum()
                errors.append(f"Field '{field}' has {negative_count} negative values")
    
    # Check logical consistency
    if all(f in df.columns for f in ['wins', 'podiums', 'races_finished', 'races_entered']):
        # wins <= podiums
        inconsistent = df['wins'] > df['podiums']
        if inconsistent.any():
            warnings.append(
                f"{inconsistent.sum()} drivers have more wins than podiums"
            )
        
        # podiums <= races_finished
        inconsistent = df['podiums'] > df['races_finished']
        if inconsistent.any():
            warnings.append(
                f"{inconsistent.sum()} drivers have more podiums than races finished"
            )
        
        # races_finished <= races_entered
        inconsistent = df['races_finished'] > df['races_entered']
        if inconsistent.any():
            errors.append(
                f"{inconsistent.sum()} drivers have more races finished than entered"
            )
    
    # Check for duplicates
    if all(f in df.columns for f in ['driver_id', 'season']):
        duplicates = df.duplicated(subset=['driver_id', 'season'], keep=False)
        if duplicates.any():
            dup_count = duplicates.sum()
            errors.append(f"Found {dup_count} duplicate (driver, season) combinations")
            logger.error(
                f"Duplicate metrics:\n{df[duplicates][['driver_id', 'season']]}"
            )
    
    # Log warnings (non-critical)
    for warning in warnings:
        logger.warning(f"Driver metrics validation warning: {warning}")
    
    if errors:
        for error in errors:
            logger.error(f"Driver metrics validation error: {error}")
        raise DataValidationError(
            f"Driver metrics validation failed with {len(errors)} errors"
        )
    
    logger.info(f"Validated {len(df)} driver metrics successfully")


def validate_constructor_metrics_df(df: pd.DataFrame) -> None:
    """
    Validate constructor metrics DataFrame.
    
    Checks:
        - Required fields are not null
        - Numeric fields are non-negative
        - No duplicate (constructor_id, season) combinations
        - Reliability rate is between 0 and 100
        - Logical consistency
    
    Args:
        df: Constructor metrics DataFrame to validate
        
    Raises:
        DataValidationError: If validation fails
    """
    if df.empty:
        logger.warning("Empty constructor metrics DataFrame provided for validation")
        return
    
    errors = []
    warnings = []
    
    # Check required fields
    required_fields = ['constructor_id', 'season', 'races_entered', 'total_points']
    for field in required_fields:
        if field not in df.columns:
            errors.append(f"Missing required field: {field}")
        elif df[field].isna().any():
            null_count = df[field].isna().sum()
            errors.append(f"Field '{field}' has {null_count} null values")
    
    # Check non-negative fields
    non_negative_fields = ['races_entered', 'podiums', 'wins', 'one_two_finishes',
                           'double_dnf', 'total_points']
    for field in non_negative_fields:
        if field in df.columns:
            if (df[field] < 0).any():
                negative_count = (df[field] < 0).sum()
                errors.append(f"Field '{field}' has {negative_count} negative values")
    
    # Check reliability rate range (0-100%)
    if 'reliability_rate' in df.columns:
        out_of_range = (df['reliability_rate'] < 0) | (df['reliability_rate'] > 100)
        if out_of_range.any():
            invalid_count = out_of_range.sum()
            errors.append(
                f"Found {invalid_count} reliability_rate values outside 0-100 range"
            )
    
    # Check logical consistency: wins <= podiums
    if all(f in df.columns for f in ['wins', 'podiums']):
        inconsistent = df['wins'] > df['podiums']
        if inconsistent.any():
            warnings.append(
                f"{inconsistent.sum()} constructors have more wins than podiums"
            )
    
    # Check for duplicates
    if all(f in df.columns for f in ['constructor_id', 'season']):
        duplicates = df.duplicated(subset=['constructor_id', 'season'], keep=False)
        if duplicates.any():
            dup_count = duplicates.sum()
            errors.append(
                f"Found {dup_count} duplicate (constructor, season) combinations"
            )
            logger.error(
                f"Duplicate metrics:\n{df[duplicates][['constructor_id', 'season']]}"
            )
    
    # Log warnings
    for warning in warnings:
        logger.warning(f"Constructor metrics validation warning: {warning}")
    
    if errors:
        for error in errors:
            logger.error(f"Constructor metrics validation error: {error}")
        raise DataValidationError(
            f"Constructor metrics validation failed with {len(errors)} errors"
        )
    
    logger.info(f"Validated {len(df)} constructor metrics successfully")