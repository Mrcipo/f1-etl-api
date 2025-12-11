import os
import sys
import logging
from typing import Optional, Tuple

import requests

# 1) Configurar Django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "f1api.settings")

import django  # noqa: E402

django.setup()

from django.db import transaction  # noqa: E402
from core.models import Race  # noqa: E402
from etl.orchestrator import run_pipeline  # noqa: E402

logger = logging.getLogger(__name__)

API_BASE_URL = "https://api.jolpi.ca/ergast/f1"


def get_last_race_from_db() -> Optional[Tuple[int, int]]:
    """
    Devuelve (season, round) de la última carrera cargada en la base,
    o None si no hay ninguna.
    """
    last_race = Race.objects.order_by("-season", "-round").first()
    if not last_race:
        logger.info("No hay carreras en la base de datos todavía.")
        return None

    logger.info(
        f"Última carrera en DB: season={last_race.season}, round={last_race.round}"
    )
    return last_race.season, last_race.round


def get_last_completed_race_from_api() -> Optional[Tuple[int, int]]:
    """
    Usa el endpoint 'current/last/results.json' para obtener la última carrera
    CON resultados disponibles (es decir, ya corrida).
    """
    url = f"{API_BASE_URL}/current/last/results.json"
    logger.info(f"Llamando a API Jolpi para última carrera completada: {url}")

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error llamando a la API Jolpi: {e}")
        return None

    data = resp.json()
    try:
        races = data["MRData"]["RaceTable"]["Races"]
        if not races:
            logger.warning("La API devolvió 0 carreras en current/last/results.")
            return None

        last_race = races[0]  # current/last debería traer solo una
        season = int(last_race["season"])
        round_ = int(last_race["round"])

        logger.info(
            f"Última carrera completada según API: season={season}, round={round_}"
        )
        return season, round_

    except (KeyError, ValueError, IndexError) as e:
        logger.error(f"Error parseando respuesta de API Jolpi: {e}")
        return None


def should_run_etl(db_last: Optional[Tuple[int, int]],
                   api_last: Optional[Tuple[int, int]]) -> bool:
    """
    Decide si hay que ejecutar el ETL.
    - Si no hay nada en DB y sí hay algo en API → correr.
    - Si la API tiene temporada más nueva → correr.
    - Si misma temporada pero round mayor → correr.
    - En cualquier otro caso → no correr.
    """
    if api_last is None:
        logger.warning("No se pudo determinar la última carrera desde la API.")
        return False

    api_season, api_round = api_last

    if db_last is None:
        logger.info("DB vacía de carreras: se ejecutará ETL.")
        return True

    db_season, db_round = db_last

    if api_season > db_season:
        logger.info(
            f"La API tiene temporada más nueva (API {api_season} > DB {db_season}). "
            "Se ejecutará ETL."
        )
        return True

    if api_season == db_season and api_round > db_round:
        logger.info(
            f"La API tiene un round más nuevo (API round {api_round} > DB round {db_round}). "
            "Se ejecutará ETL."
        )
        return True

    logger.info("La base de datos ya está al día con la última carrera completada.")
    return False


def main():
    logger.info("=" * 70)
    logger.info("Iniciando chequeo inteligente F1 ETL")
    logger.info("=" * 70)

    db_last = get_last_race_from_db()
    api_last = get_last_completed_race_from_api()

    if not should_run_etl(db_last, api_last):
        logger.info("No se ejecuta ETL: no hay nuevas carreras.")
        logger.info("=" * 70)
        return

    logger.info("Ejecutando ETL en modo incremental...")
    try:
        with transaction.atomic():
            # Podrías usar mode="incremental" si ya lo tenés implementado.
            # Si preferís, podés usar mode="season" con [api_season].
            result = run_pipeline(
                mode="incremental",
                seasons=None,
                save_raw=False,
            )

        status = result.get("status", "UNKNOWN")
        logger.info(f"ETL finalizó con estado: {status}")
        logger.info(f"Detalle: {result}")
    except Exception as e:
        logger.exception(f"Error ejecutando el ETL incremental: {e}")
        # Si querés que el scheduler vea el error, salimos con código 1
        sys.exit(1)

    logger.info("Chequeo inteligente completado.")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
