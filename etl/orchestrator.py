"""
Orchestrator for the full F1 ETL pipeline.

This module coordinates:
- which seasons to process
- calling extract → transform → load in order
- tracking ETLRun records
"""

from typing import Iterable, Optional

from etl.config import DEFAULT_SEASONS_RANGE
from etl.extract.ergast_client import ErgastClient
from etl.extract.extractors import (
    extract_season_races,
    extract_season_results,
    extract_season_qualifying,
    extract_standings_for_season,
)


def run_pipeline(
    seasons: Optional[Iterable[int]] = None,
    incremental: bool = False,
    backfill: bool = False,
) -> None:
    """
    Main orchestration entry point for the F1 ETL pipeline.

    Parameters:
        seasons: Iterable of season years to process. If None, uses DEFAULT_SEASONS_RANGE.
        incremental: Flag indicating incremental mode (e.g., only latest season / rounds).
        backfill: Flag indicating full historical reload for the provided seasons.

    For now this is a stub – later we will:
        - Create an ErgastClient
        - Loop over seasons
        - Call extract → transform → load
        - Create ETLRun records to track executions
    """
    if seasons is None:
        seasons = DEFAULT_SEASONS_RANGE

    # TODO: implement orchestration logic in a later phase
    client = ErgastClient()
    _ = client  # avoid unused variable warning for now
    _ = seasons
    _ = incremental
    _ = backfill