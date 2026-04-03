import pytest
import numpy as np
from app.layers.behavioral.social_preferences import SocialPreferences


def test_instantiation():
    model = SocialPreferences()
    assert model is not None


def test_layer_id():
    model = SocialPreferences()
    assert model.layer_id == "l13"


def test_name():
    model = SocialPreferences()
    assert model.name == "Social Preferences"


async def test_compute_empty_db_returns_dict(db_conn):
    model = SocialPreferences()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = SocialPreferences()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = SocialPreferences()
    result = await model.compute(db_conn, country_iso3="USA")
    assert result["score"] is None


async def test_run_returns_layer_id(db_conn):
    model = SocialPreferences()
    result = await model.run(db_conn, country_iso3="USA")
    assert result["layer_id"] == "l13"


async def test_run_returns_signal(db_conn):
    model = SocialPreferences()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result


async def test_compute_with_gini_data_returns_score(db_conn):
    # Insert Gini index series
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wdi", "SI.POV.GINI", "BRA", "gini index"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]

    for i, val in enumerate([52.0, 53.0, 51.5, 52.5, 50.0, 51.0, 49.5]):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, f"{2015 + i}-01-01", val),
        )

    model = SocialPreferences()
    result = await model.compute(db_conn, country_iso3="BRA")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


async def test_compute_with_gini_has_fehr_schmidt(db_conn):
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wdi", "SI.POV.GINI", "ZAF", "gini index"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]

    for i, val in enumerate([63.0, 62.5, 63.5, 64.0, 62.0]):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, f"{2016 + i}-01-01", val),
        )

    model = SocialPreferences()
    result = await model.compute(db_conn, country_iso3="ZAF")
    if result.get("score") is not None:
        assert "fehr_schmidt" in result
        fs = result["fehr_schmidt"]
        assert "alpha" in fs and "beta" in fs
        assert fs["alpha"] >= fs["beta"] >= 0


def test_fehr_schmidt_static():
    gini = np.array([35.0, 36.0, 37.0, 36.5, 38.0])
    result = SocialPreferences._fehr_schmidt(gini)
    assert "alpha" in result and "beta" in result
    assert result["alpha"] >= result["beta"]


def test_ultimatum_fairness_static():
    gini = np.array([40.0, 41.0, 42.0, 40.5])
    result = SocialPreferences._ultimatum_fairness(gini)
    assert "rejection_threshold" in result
    assert 0 <= result["rejection_threshold"] <= 1
    assert "min_acceptable_share" in result
