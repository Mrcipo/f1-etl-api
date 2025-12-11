"""
Extract phase of the F1 ETL pipeline.

This module is responsible for:
- Interacting with the Ergast API
- Handling rate limiting and retries
- (Opcional) Re-exportar helpers de alto nivel
"""

from .ergast_client import ErgastClient

__all__ = ["ErgastClient"]
