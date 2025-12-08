"""
Load phase of the F1 ETL pipeline.

Responsible for:
- Mapping cleaned DataFrames into Django ORM models
- Performing bulk inserts / updates (UPSERT patterns)
- Ensuring idempotency of loads per race/season
"""