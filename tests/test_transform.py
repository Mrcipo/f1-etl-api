"""
Unit tests for the transform layer (parsers, cleaners, metric calculators).

These run on plain pandas DataFrames and need no database.
"""
import pandas as pd
import pytest

from etl.transform.calculators import compute_constructor_metrics, compute_driver_metrics
from etl.transform.cleaners import clean_qualifying_df, clean_races_df, clean_results_df
from etl.transform.parsers import (
    parse_constructor_standings_json,
    parse_driver_standings_json,
    parse_qualifying_json,
    parse_races_json,
    parse_results_json,
)
from etl.transform.validators import DataValidationError, validate_results_df
from tests import fixtures as fx


def _season_results():
    frames = [parse_results_json(fx.results_json(rnd), fx.SEASON, rnd) for rnd in fx.RESULTS]
    return clean_results_df(pd.concat(frames, ignore_index=True))


def _season_qualifying():
    frames = [parse_qualifying_json(fx.qualifying_json(rnd), fx.SEASON, rnd) for rnd in fx.RESULTS]
    return clean_qualifying_df(pd.concat(frames, ignore_index=True))


def test_parse_races_flattens_circuit_and_location():
    df = parse_races_json(fx.races_json())

    assert list(df["round"]) == [1, 2]
    first = df.iloc[0]
    assert first["circuit_id"] == "bahrain"
    assert first["location"] == "Sakhir"
    assert first["country"] == "Bahrain"


def test_parse_races_returns_empty_frame_on_unexpected_shape():
    assert parse_races_json({"MRData": {}}).empty
    assert parse_races_json({"MRData": {"RaceTable": {"Races": []}}}).empty


def test_clean_races_converts_coordinates_to_float():
    df = clean_races_df(parse_races_json(fx.races_json()))

    assert df["latitude"].dtype == float
    assert df.iloc[0]["latitude"] == pytest.approx(26.0325)


def test_parse_results_keeps_dnf_without_position():
    df = parse_results_json(fx.results_json(1), fx.SEASON, 1)

    assert len(df) == 3
    alonso = df[df["driver_id"] == "alonso"].iloc[0]
    assert alonso["position"] is None
    assert alonso["position_text"] == "R"
    assert alonso["status"] == "Engine"


def test_clean_results_computes_position_change_and_nullable_position():
    df = _season_results()

    verstappen_r2 = df[(df["driver_id"] == "max_verstappen") & (df["round"] == 2)].iloc[0]
    # Started 15th, finished 2nd -> gained 13 places.
    assert verstappen_r2["position_change"] == 13

    alonso_r1 = df[(df["driver_id"] == "alonso") & (df["round"] == 1)].iloc[0]
    assert pd.isna(alonso_r1["position"])
    assert pd.isna(alonso_r1["position_change"])


def test_cleaned_results_pass_validation():
    validate_results_df(_season_results())


def test_validation_rejects_results_without_driver_id():
    df = _season_results()
    df.loc[0, "driver_id"] = None

    with pytest.raises(DataValidationError):
        validate_results_df(df)


def test_parse_standings():
    drivers = parse_driver_standings_json(fx.driver_standings_json(), fx.SEASON)
    constructors = parse_constructor_standings_json(fx.constructor_standings_json(), fx.SEASON)

    assert list(drivers["driver_id"]) == ["max_verstappen", "perez", "alonso"]
    assert set(drivers["round"]) == {2}
    assert constructors.iloc[0]["constructor_id"] == "red_bull"
    assert constructors.iloc[0]["points"] == 87.0


def test_compute_driver_metrics():
    metrics = compute_driver_metrics(_season_results(), _season_qualifying()).set_index("driver_id")

    ver = metrics.loc["max_verstappen"]
    assert ver["races_entered"] == 2
    assert ver["wins"] == 1
    assert ver["podiums"] == 2
    assert ver["poles"] == 1
    assert ver["total_points"] == 44
    assert ver["avg_finish_position"] == 1.5

    alo = metrics.loc["alonso"]
    assert alo["races_finished"] == 1
    assert alo["dnf_count"] == 1
    assert alo["podiums"] == 1


def test_compute_driver_metrics_empty_input():
    assert compute_driver_metrics(pd.DataFrame(), pd.DataFrame()).empty


def test_compute_constructor_metrics():
    metrics = compute_constructor_metrics(_season_results()).set_index("constructor_id")

    red_bull = metrics.loc["red_bull"]
    assert red_bull["races_entered"] == 2
    assert red_bull["wins"] == 2
    assert red_bull["one_two_finishes"] == 2
    assert red_bull["total_points"] == 87

    aston = metrics.loc["aston_martin"]
    assert aston["wins"] == 0
    # A single car retiring is not a double DNF.
    assert aston["double_dnf"] == 0
    assert aston["reliability_rate"] == pytest.approx(50.0)
