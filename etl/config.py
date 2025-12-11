# etl/config.py
from pathlib import Path

# Directorio base del módulo etl
BASE_ETL_DIR = Path(__file__).resolve().parent
RAW_DATA_DIR = BASE_ETL_DIR.parent / "data" / "raw"

# Ergast API
ERGAST_BASE_URL = "https://api.jolpi.ca/ergast/f1"

# Temporadas a procesar (puedes ajustar estas constantes más adelante)
START_SEASON = 2010
END_SEASON = 2024
DEFAULT_SEASONS = list(range(START_SEASON, END_SEASON + 1))

# Rate limiting y reintentos
REQUEST_DELAY_SECONDS = 1.0       # segundos entre requests/reintentos
MAX_RETRIES = 3                   # número máximo de reintentos
BACKOFF_FACTOR = 2.0              # factor de backoff exponencial

# Guardar o no los JSON crudos
SAVE_RAW_JSON = True
