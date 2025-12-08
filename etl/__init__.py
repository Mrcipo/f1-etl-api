"""
ETL package for the F1 data pipeline.

This package is organized into three main phases:
- extract:  Responsible for calling the Ergast API and persisting raw JSON.
- transform: Responsible for converting raw JSON into cleaned, structured data.
- load:      Responsible for loading transformed data into Django models / database.
"""

