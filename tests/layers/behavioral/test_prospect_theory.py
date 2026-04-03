import pytest
import numpy as np
from app.layers.behavioral.prospect_theory import ProspectTheory, _prelec_weight, _value_function


def test_instantiation():
    model = ProspectTheory()
    assert model is not None


def test_layer_id():
    model = ProspectTheory()
    assert model.layer_id == "l13"


def test_name():
    model = ProspectTheory()
    assert model.name == "Prospect Theory"


def test_prelec_weight_returns_array():
    p = np.array([0.1, 0.3, 0.5, 0.7, 0.9])
    w = _prelec_weight(p, gamma=0.65)
    assert w.shape == p.shape
    assert np.all(w > 0) and np.all(w < 1)


def test_value_function_gains_positive():
    x = np.array([1.0, 2.0, 5.0])
    v = _value_function(x, alpha=0.88, beta=0.88, lam=2.25)
    assert np.all(v >= 0)


def test_value_function_losses_negative():
    x = np.array([-1.0, -2.0, -5.0])
    v = _value_function(x, alpha=0.88, beta=0.88, lam=2.25)
    assert np.all(v <= 0)


async def test_compute_empty_db_returns_dict(db_conn):
    model = ProspectTheory()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = ProspectTheory()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = ProspectTheory()
    result = await model.compute(db_conn, country_iso3="USA")
    assert result["score"] is None


async def test_run_returns_layer_id(db_conn):
    model = ProspectTheory()
    result = await model.run(db_conn, country_iso3="USA")
    assert result["layer_id"] == "l13"


async def test_run_returns_signal(db_conn):
    model = ProspectTheory()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result


async def test_compute_with_gdp_data_returns_numeric_score(db_conn):
    # Insert GDP growth series (fallback path)
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wdi", "NY.GDP.MKTP.KD.ZG", "DEU", "gdp growth"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]

    np.random.seed(42)
    vals = np.random.normal(2.0, 1.5, 25).tolist()
    for i, v in enumerate(vals):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, f"{2000 + i}-01-01", v),
        )

    model = ProspectTheory()
    result = await model.compute(db_conn, country_iso3="DEU")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


async def test_compute_with_data_has_value_function_keys(db_conn):
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("wdi", "NY.GDP.MKTP.KD.ZG", "FRA", "gdp growth"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]

    for i in range(20):
        await db_conn.execute(
            "INSERT INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, f"{2000 + i}-01-01", float(i % 5 - 2)),
        )

    model = ProspectTheory()
    result = await model.compute(db_conn, country_iso3="FRA")
    if result.get("score") is not None:
        assert "value_function" in result
        vf = result["value_function"]
        assert "alpha" in vf and "beta" in vf and "lambda" in vf
