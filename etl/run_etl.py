"""
CLI entry point for F1 ETL pipeline.

Usage:
    python -m etl.run_etl --mode backfill
    python -m etl.run_etl --mode backfill --seasons 2010 2011 2012
    python -m etl.run_etl --mode incremental
    python -m etl.run_etl --mode season --seasons 2023
"""
import os
import sys
import argparse
import logging
from typing import List, Optional

# Initialize Django before importing models
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "f1api.settings")
django.setup()

from etl.orchestrator import run_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('etl_pipeline.log'),
    ]
)

logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description='F1 ETL Pipeline - Extract, Transform, and Load F1 data from Ergast API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill all seasons (from config)
  python -m etl.run_etl --mode backfill

  # Backfill specific seasons
  python -m etl.run_etl --mode backfill --seasons 2010 2011 2012

  # Process current season (incremental)
  python -m etl.run_etl --mode incremental

  # Process specific seasons
  python -m etl.run_etl --mode season --seasons 2023 2024

  # Save raw JSON files
  python -m etl.run_etl --mode incremental --save-raw
        """
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        required=True,
        choices=['backfill', 'season', 'incremental'],
        help='Execution mode: backfill (historical), season (specific), or incremental (current)'
    )
    
    parser.add_argument(
        '--seasons',
        type=int,
        nargs='+',
        help='List of seasons to process (required for "season" mode)'
    )
    
    parser.add_argument(
        '--save-raw',
        action='store_true',
        help='Save raw JSON responses to disk'
    )
    
    return parser.parse_args()


def print_summary(result: dict) -> None:
    """
    Print pipeline execution summary in a readable format.
    
    Args:
        result: Dictionary with pipeline execution results
    """
    print("\n" + "="*70)
    print("ETL PIPELINE EXECUTION SUMMARY")
    print("="*70)
    print(f"Mode:                    {result['mode']}")
    print(f"Status:                  {result['status']}")
    print(f"Duration:                {result['duration_seconds']:.2f} seconds")
    print(f"\nSeasons Processed:       {', '.join(map(str, result['seasons']))}")
    print(f"Total Races Processed:   {result['total_races_processed']}")
    print(f"Total Drivers:           {result['total_drivers']}")
    print(f"Total Constructors:      {result['total_constructors']}")
    print("="*70 + "\n")


def main() -> None:
    """
    Main entry point for the ETL pipeline CLI.
    """
    try:
        # Parse arguments
        args = parse_arguments()
        
        logger.info("Starting F1 ETL Pipeline")
        logger.info(f"Arguments: mode={args.mode}, seasons={args.seasons}, save_raw={args.save_raw}")
        
        # Validate arguments
        if args.mode == 'season' and not args.seasons:
            print("Error: --seasons is required when using mode 'season'")
            sys.exit(1)
        
        # Run pipeline
        result = run_pipeline(
            mode=args.mode,
            seasons=args.seasons,
            save_raw=args.save_raw,
        )
        
        # Print summary
        print_summary(result)
        
        # Exit with appropriate code
        if result['status'] == 'SUCCESS':
            logger.info("Pipeline completed successfully")
            sys.exit(0)
        elif result['status'] == 'PARTIAL':
            logger.warning("Pipeline completed with some errors")
            sys.exit(0)
        else:
            logger.error("Pipeline failed")
            sys.exit(1)
            
    except ValueError as e:
        print(f"Error: {e}")
        logger.error(f"Validation error: {e}")
        sys.exit(1)
        
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
        logger.warning("Pipeline interrupted by user")
        sys.exit(130)
        
    except Exception as e:
        print(f"Critical error: {e}")
        logger.error(f"Critical error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()