"""
End-to-end tests: run the full ETL against a fake Ergast client, load into
PostgreSQL and read the data back through the REST API.
"""
from unittest import mock

import pytest
from rest_framework.test import APIClient

from core.models import (
    Circuit,
    Constructor,
    ConstructorMetrics,
    Driver,
    DriverMetrics,
    DriverStanding,
    ETLRun,
    Qualifying,
    Race,
    Result,
)
from etl.orchestrator import determine_seasons_to_process, run_pipeline
from tests import fixtures as fx

pytestmark = pytest.mark.django_db


@pytest.fixture
def loaded_season():
    with mock.patch("etl.orchestrator.ErgastClient", fx.FakeErgastClient):
        return run_pipeline(mode="season", seasons=[fx.SEASON], save_raw=False)


def test_determine_seasons_to_process():
    assert determine_seasons_to_process("season", [2024, 2022]) == [2022, 2024]
    with pytest.raises(ValueError):
        determine_seasons_to_process("season", None)
    with pytest.raises(ValueError):
        determine_seasons_to_process("nope")


def test_pipeline_loads_every_table(loaded_season):
    assert loaded_season["status"] == "SUCCESS"
    assert loaded_season["seasons"] == [fx.SEASON]
    assert loaded_season["total_races_processed"] == 2

    assert Driver.objects.count() == 3
    assert Constructor.objects.count() == 2
    assert Circuit.objects.count() == 2
    assert Race.objects.filter(season=fx.SEASON).count() == 2
    assert Result.objects.count() == 6
    assert Qualifying.objects.count() == 6
    assert DriverStanding.objects.count() == 3
    assert DriverMetrics.objects.count() == 3
    assert ConstructorMetrics.objects.count() == 2

    run = ETLRun.objects.get()
    assert run.status == "SUCCESS"
    assert run.seasons_processed == [fx.SEASON]


def test_pipeline_is_idempotent(loaded_season):
    with mock.patch("etl.orchestrator.ErgastClient", fx.FakeErgastClient):
        second = run_pipeline(mode="season", seasons=[fx.SEASON], save_raw=False)

    assert second["status"] == "SUCCESS"
    assert Driver.objects.count() == 3
    assert Race.objects.count() == 2
    assert Result.objects.count() == 6
    assert DriverMetrics.objects.count() == 3


def test_dnf_is_stored_without_position(loaded_season):
    dnf = Result.objects.get(driver_id="alonso", race__round=1)
    assert dnf.position is None
    assert dnf.status == "Engine"


def test_api_lists_drivers(loaded_season):
    response = APIClient().get("/api/v1/drivers/")

    assert response.status_code == 200
    assert response.json()["count"] == 3


def test_api_driver_standings(loaded_season):
    client = APIClient()

    assert client.get("/api/v1/standings/drivers/").status_code == 400

    response = client.get("/api/v1/standings/drivers/", {"season": fx.SEASON})
    assert response.status_code == 200
    standings = response.json()["standings"]
    assert [s["position"] for s in standings] == [1, 2, 3]


def test_api_compare_drivers(loaded_season):
    response = APIClient().get(
        "/api/v1/analytics/compare/drivers/",
        {"driver_ids": "max_verstappen,perez", "season": fx.SEASON},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["comparison"]["total_points_gap"] == 1
    assert body["comparison"]["wins_difference"] == 0
