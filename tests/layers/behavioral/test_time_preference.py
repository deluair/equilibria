import pytest
import numpy as np
from app.layers.behavioral.time_preference import TimePreference


def test_instantiation():
    model = TimePreference()
    assert model is not None


def test_layer_id():
    model = TimePreference()
    assert model.layer_id == "l13"


def test_name():
    model = TimePreference()
    assert model.name == "Time Preference"


async def test_compute_empty_db_returns_dict(db_conn):
    model = TimePreference()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = TimePreference()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = TimePreference()
    result = await model.compute(db_conn, country_iso3="USA")
    assert result["score"] is None


async def test_run_returns_layer_id(db_conn):
    model = TimePreference()
    result = await model.run(db_conn, country_iso3="USA")
    assert result["layer_id"] == "l13"


async def test_run_returns_signal(db_conn):
    model = TimePreference()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result


async def test_compute_with_savings_data_returns_score(db_conn):
    # Insert personal savings rate (PSAVERT) series
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("fred", "PSAVERT", "USA", "personal savings rate"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]

    np.random.seed(1)
    for i in range(20):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, f"{2000 + i}-01-01", max(2.0, 8.0 + np.random.normal(0, 1.5))),
        )

    model = TimePreference()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


async def test_compute_with_savings_has_beta_delta(db_conn):
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("fred", "PSAVERT", "CAN", "personal savings rate"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]

    for i in range(15):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, f"{2005 + i}-01-01", 7.0 + float(i) * 0.1),
        )

    model = TimePreference()
    result = await model.compute(db_conn, country_iso3="CAN")
    if result.get("score") is not None:
        assert "beta_delta" in result
        bd = result["beta_delta"]
        assert "beta" in bd and "delta" in bd
        assert 0 < bd["beta"] <= 1
        assert 0 < bd["delta"] <= 1


def test_estimate_hyperbolic_static():
    savings = np.array([10, 9, 8, 7, 9, 8, 10, 9, 7, 8, 9, 10, 8, 7, 9], dtype=float)
    result = TimePreference._estimate_hyperbolic(savings)
    assert "k" in result
    assert result["k"] > 0


def test_retirement_implications_static():
    savings = np.array([8.0] * 20)
    result = TimePreference._retirement_implications(savings, beta=0.70, delta=0.96)
    assert "welfare_loss_pct" in result
    assert "retirement_shortfall_pct" in result
    assert result["retirement_shortfall_pct"] >= 0
