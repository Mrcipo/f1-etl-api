"""
Higher-level loading functions that receive cleaned/validated DataFrames
and persist them into the PostgreSQL database via Django ORM.
"""

import pandas as pd


def load_reference_entities(drivers_df: pd.DataFrame, constructors_df: pd.DataFrame, circuits_df: pd.DataFrame) -> None:
    """
    Load or update master data: drivers, constructors, circuits.

    Strategy (later):
    - Use upsert semantics keyed by natural IDs (driver_id, constructor_id, circuit_id)
    """
    return


def load_races_and_results(
    races_df: pd.DataFrame,
    results_df: pd.DataFrame,
    qualifying_df: pd.DataFrame,
) -> None:
    """
    Load races, qualifying, and race results into the database.

    Strategy (later):
    - Ensure Race exists (season, round)
    - Replace Qualifying / Result for a race on re-run
    """
    return


def load_standings(
    driver_standings_df: pd.DataFrame,
    constructor_standings_df: pd.DataFrame,
) -> None:
    """
    Load driver and constructor standings tables.
    """
    return


def load_metrics(
    driver_metrics_df: pd.DataFrame,
    constructor_metrics_df: pd.DataFrame,
) -> None:
    """
    Load aggregated metrics into DriverMetrics and ConstructorMetrics.
    """
    return