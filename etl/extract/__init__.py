"""
Extract phase of the F1 ETL pipeline.

This module is responsible for:
- Interacting with the Ergast API
- Handling rate limiting and retries
- Persisting raw JSON responses to disk
"""

from .ergast_client import ErgastClient  # noqa
from .extractors import (               # noqa
    extract_season_races,
    extract_season_results,
    extract_season_qualifying,
    extract_standings_for_season,
)
