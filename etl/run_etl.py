"""
Simple CLI entrypoint to run the F1 ETL pipeline manually.

Later this can be wired to:
- a cron job
- a Django management command
- or an external scheduler like Prefect / Airflow
"""

import argparse
from typing import List, Optional

from etl.orchestrator import run_pipeline


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse CLI arguments for the ETL runner.

    Supported options (for now):
    - --seasons: explicit list of seasons
    - --incremental: run in incremental mode
    - --backfill: run in backfill mode
    """
    parser = argparse.ArgumentParser(description="Run F1 ETL pipeline.")
    parser.add_argument(
        "--seasons",
        nargs="*",
        type=int,
        help="List of seasons to process, e.g. --seasons 2022 2023",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Run the pipeline in incremental mode.",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Run the pipeline in backfill (full reload) mode.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    """
    Main function: parse arguments and dispatch to run_pipeline.
    """
    args = parse_args(argv)
    run_pipeline(
        seasons=args.seasons,
        incremental=args.incremental,
        backfill=args.backfill,
    )


if __name__ == "__main__":
    main()