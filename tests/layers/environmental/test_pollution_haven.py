import json
import pytest
from app.layers.environmental.pollution_haven import PollutionHaven


def test_instantiation():
    assert PollutionHaven() is not None


def test_layer_id():
    assert PollutionHaven.layer_id == "l9"


def test_name():
    assert PollutionHaven().name == "Pollution Haven"


async def test_compute_empty_db_returns_unavailable(db_conn):
    result = await PollutionHaven().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


async def test_compute_insufficient_rows_unavailable(db_conn):
    # Insert only 5 rows - below the 20-row threshold
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?,?,?,?)",
        ("gravity", "TRADE_TEST", "BGD", "Trade test"),
    )
    sid = (await db_conn.fetch_one("SELECT id FROM data_series WHERE series_id='TRADE_TEST'"))["id"]
    meta = json.dumps({"gdp_origin": 1e11, "gdp_dest": 2e11, "distance": 5000})
    for yr in range(2000, 2005):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?,?,?)",
            (sid, f"{yr}-01-01", float(yr * 1000)),
        )
    result = await PollutionHaven().compute(db_conn, country_iso3="BGD")
    assert result.get("score") is None


def test_decompose_effects_keys():
    result = PollutionHaven._decompose_effects(
        beta_es=-0.5,
        beta_dirty=0.3,
        beta_interaction=-0.2,
        mean_es_diff=1.0,
        dirty_share=0.3,
    )
    for key in ("scale_effect", "composition_effect", "technique_effect", "net_effect", "dominant"):
        assert key in result


def test_decompose_effects_dominant_composition():
    result = PollutionHaven._decompose_effects(
        beta_es=0.0,
        beta_dirty=0.0,
        beta_interaction=-5.0,
        mean_es_diff=1.0,
        dirty_share=0.1,
    )
    assert result["dominant"] == "composition"
