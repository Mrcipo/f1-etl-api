"""
Django management command to run the F1 ETL pipeline.

This command provides a Django-native interface to execute the ETL pipeline
with different modes (backfill, incremental, season-specific).

Examples:
    python manage.py run_etl --mode backfill
    python manage.py run_etl --mode season --seasons 2021 2022 2023
    python manage.py run_etl --mode incremental
    python manage.py run_etl --mode incremental --save-raw

Cron scheduling examples:
    # Run incremental ETL daily at 02:00 UTC
    0 2 * * * /usr/bin/python /path/to/project/manage.py run_etl --mode incremental >> /var/log/f1_etl.log 2>&1
    
    # Run incremental ETL every Monday at 03:00 UTC (during off-season)
    0 3 * * 1 /usr/bin/python /path/to/project/manage.py run_etl --mode incremental >> /var/log/f1_etl.log 2>&1
    
    # Run full backfill on first day of each month at 04:00 UTC
    0 4 1 * * /usr/bin/python /path/to/project/manage.py run_etl --mode backfill >> /var/log/f1_etl_backfill.log 2>&1
"""
import logging
import sys
from typing import List, Optional

from django.core.management.base import BaseCommand, CommandError

from etl.orchestrator import run_pipeline

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Management command to execute the F1 ETL pipeline.
    
    This command orchestrates the Extract-Transform-Load process for F1 data
    from the Ergast API into the database. It supports three execution modes:
    
    - backfill: Process historical data for multiple seasons
    - season: Process specific seasons explicitly provided
    - incremental: Process only the current/latest season
    """
    
    help = (
        'Run the F1 ETL pipeline to extract, transform, and load Formula 1 data.\n\n'
        'Modes:\n'
        '  backfill    - Process historical seasons (default range from config)\n'
        '  season      - Process specific seasons (requires --seasons)\n'
        '  incremental - Process only current season\n\n'
        'Examples:\n'
        '  python manage.py run_etl --mode backfill\n'
        '  python manage.py run_etl --mode season --seasons 2021 2022 2023\n'
        '  python manage.py run_etl --mode incremental\n'
        '  python manage.py run_etl --mode incremental --save-raw'
    )
    
    def add_arguments(self, parser):
        """Add command line arguments."""
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
            help='List of seasons to process (required for "season" mode, optional for "backfill")'
        )
        
        parser.add_argument(
            '--save-raw',
            action='store_true',
            help='Save raw JSON responses to disk for debugging/archival'
        )
    
    def handle(self, *args, **options):
        """Execute the ETL pipeline with provided options."""
        mode = options['mode']
        seasons = options.get('seasons')
        save_raw = options.get('save_raw', False)
        
        # Validate arguments
        if mode == 'season' and not seasons:
            raise CommandError(
                'Mode "season" requires --seasons argument.\n'
                'Example: python manage.py run_etl --mode season --seasons 2022 2023'
            )
        
        # Log execution start
        logger.info("="*70)
        logger.info("Starting F1 ETL Pipeline via Django management command")
        logger.info(f"Mode: {mode}")
        if seasons:
            logger.info(f"Seasons: {seasons}")
        logger.info(f"Save raw data: {save_raw}")
        logger.info("="*70)
        
        # Display initial message
        self.stdout.write(self.style.MIGRATE_HEADING("\n" + "="*70))
        self.stdout.write(self.style.MIGRATE_HEADING("F1 ETL Pipeline Execution"))
        self.stdout.write(self.style.MIGRATE_HEADING("="*70))
        self.stdout.write(f"Mode:        {self.style.WARNING(mode)}")
        if seasons:
            self.stdout.write(f"Seasons:     {self.style.WARNING(', '.join(map(str, seasons)))}")
        self.stdout.write(f"Save raw:    {self.style.WARNING(str(save_raw))}")
        self.stdout.write(self.style.MIGRATE_HEADING("="*70 + "\n"))
        
        try:
            # Execute pipeline
            self.stdout.write("Running ETL pipeline...")
            logger.info(f"Calling run_pipeline with mode={mode}, seasons={seasons}, save_raw={save_raw}")
            
            result = run_pipeline(
                mode=mode,
                seasons=seasons,
                save_raw=save_raw
            )
            
            # Display results
            self._display_results(result)
            
            # Log completion
            logger.info("ETL Pipeline completed successfully")
            logger.info(f"Final status: {result['status']}")
            
            # Exit with appropriate code
            if result['status'] == 'SUCCESS':
                sys.exit(0)
            elif result['status'] == 'PARTIAL':
                self.stdout.write(
                    self.style.WARNING(
                        "\nWarning: Pipeline completed with partial success. "
                        "Check logs for details."
                    )
                )
                sys.exit(0)
            else:
                self.stdout.write(
                    self.style.ERROR(
                        "\nError: Pipeline failed. Check logs for details."
                    )
                )
                sys.exit(1)
                
        except ValueError as e:
            logger.error(f"Validation error: {e}", exc_info=True)
            raise CommandError(f"Validation error: {e}")
            
        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted by user")
            self.stdout.write(
                self.style.WARNING("\n\nPipeline interrupted by user")
            )
            sys.exit(130)
            
        except Exception as e:
            logger.error(f"Critical error in ETL pipeline: {e}", exc_info=True)
            self.stdout.write(
                self.style.ERROR(f"\n\nCritical error: {e}")
            )
            self.stdout.write(
                self.style.ERROR("Check logs for detailed error information.")
            )
            raise CommandError(f"ETL pipeline failed: {e}")
    
    def _display_results(self, result: dict) -> None:
        """
        Display pipeline execution results in a formatted output.
        
        Args:
            result: Dictionary with pipeline execution results
        """
        self.stdout.write("\n" + "="*70)
        self.stdout.write(self.style.MIGRATE_HEADING("ETL PIPELINE EXECUTION SUMMARY"))
        self.stdout.write("="*70)
        
        # Status with color coding
        status = result.get('status', 'UNKNOWN')
        if status == 'SUCCESS':
            status_display = self.style.SUCCESS(status)
        elif status == 'PARTIAL':
            status_display = self.style.WARNING(status)
        else:
            status_display = self.style.ERROR(status)
        
        self.stdout.write(f"Status:                  {status_display}")
        self.stdout.write(f"Mode:                    {result.get('mode', 'N/A')}")
        
        # Duration
        duration = result.get('duration_seconds', 0)
        duration_display = self._format_duration(duration)
        self.stdout.write(f"Duration:                {duration_display}")
        
        # Seasons processed
        seasons = result.get('seasons', [])
        if seasons:
            seasons_str = ', '.join(map(str, seasons))
            self.stdout.write(f"Seasons Processed:       {self.style.SUCCESS(seasons_str)}")
        else:
            self.stdout.write(f"Seasons Processed:       {self.style.ERROR('None')}")
        
        # Statistics
        self.stdout.write(f"Total Races Processed:   {self.style.SUCCESS(str(result.get('total_races_processed', 0)))}")
        self.stdout.write(f"Total Drivers:           {self.style.SUCCESS(str(result.get('total_drivers', 0)))}")
        self.stdout.write(f"Total Constructors:      {self.style.SUCCESS(str(result.get('total_constructors', 0)))}")
        
        self.stdout.write("="*70 + "\n")
        
        # Additional status message
        if status == 'SUCCESS':
            self.stdout.write(
                self.style.SUCCESS("✓ Pipeline completed successfully!")
            )
        elif status == 'PARTIAL':
            self.stdout.write(
                self.style.WARNING("⚠ Pipeline completed with some errors. Check logs for details.")
            )
        else:
            self.stdout.write(
                self.style.ERROR("✗ Pipeline failed. Check logs for details.")
            )
    
    def _format_duration(self, seconds: float) -> str:
        """
        Format duration in seconds to a human-readable string.
        
        Args:
            seconds: Duration in seconds
            
        Returns:
            Formatted duration string
        """
        if seconds < 60:
            return f"{seconds:.2f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.2f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.2f} hours"