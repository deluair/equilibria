import pytest
import numpy as np
from app.layers.behavioral.market_anomalies import MarketAnomalies, _newey_west_se


def test_instantiation():
    model = MarketAnomalies()
    assert model is not None


def test_layer_id():
    model = MarketAnomalies()
    assert model.layer_id == "l13"


def test_name():
    model = MarketAnomalies()
    assert model.name == "Market Anomalies"


def test_newey_west_se_shape():
    n, k = 30, 2
    X = np.column_stack([np.ones(n), np.random.randn(n)])
    resid = np.random.randn(n)
    se = _newey_west_se(X, resid, max_lag=4)
    assert se.shape == (k,)
    assert np.all(se >= 0)


async def test_compute_empty_db_returns_dict(db_conn):
    model = MarketAnomalies()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score_key(db_conn):
    model = MarketAnomalies()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result


async def test_compute_empty_db_score_is_none(db_conn):
    model = MarketAnomalies()
    result = await model.compute(db_conn, country_iso3="USA")
    assert result["score"] is None


async def test_run_returns_layer_id(db_conn):
    model = MarketAnomalies()
    result = await model.run(db_conn, country_iso3="USA")
    assert result["layer_id"] == "l13"


async def test_run_returns_signal(db_conn):
    model = MarketAnomalies()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result


async def test_compute_with_market_data_returns_score(db_conn):
    # Insert stock market index series with 30+ data points
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("fred", "SP500_LEVEL", "USA", "SP500 market index"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]

    np.random.seed(99)
    price = 1000.0
    for i in range(36):
        month = (i % 12) + 1
        price *= (1 + np.random.normal(0.005, 0.04))
        date_str = f"{2020 + i // 12}-{month:02d}-01"
        await db_conn.execute(
            "INSERT OR IGNORE INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, date_str, round(price, 2)),
        )

    model = MarketAnomalies()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


async def test_compute_with_data_has_anomaly_subkeys(db_conn):
    await db_conn.execute(
        "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?, ?, ?, ?)",
        ("fred", "MKINDEX", "GBR", "MARKET index"),
    )
    cursor = await db_conn.conn.execute("SELECT last_insert_rowid()")
    row = await cursor.fetchone()
    sid = row[0]

    np.random.seed(5)
    price = 500.0
    for i in range(36):
        month = (i % 12) + 1
        price *= (1 + np.random.normal(0.003, 0.03))
        date_str = f"{2020 + i // 12}-{month:02d}-01"
        await db_conn.execute(
            "INSERT OR IGNORE INTO data_points (series_id, date, value) VALUES (?, ?, ?)",
            (sid, date_str, round(price, 2)),
        )

    model = MarketAnomalies()
    result = await model.compute(db_conn, country_iso3="GBR")
    if result.get("score") is not None:
        assert "calendar_effects" in result
        assert "momentum_reversal" in result
        assert "post_announcement_drift" in result
        assert "limits_to_arbitrage" in result


def test_momentum_reversal_static():
    np.random.seed(42)
    returns = np.random.normal(0.001, 0.02, 60)
    result = MarketAnomalies._momentum_reversal(returns)
    assert "momentum_significant" in result
    assert "reversal_significant" in result
    assert "autocorrelation_profile" in result


def test_limits_to_arbitrage_static():
    np.random.seed(3)
    returns = np.random.normal(0, 0.02, 50)
    result = MarketAnomalies._limits_to_arbitrage(returns)
    assert "arbitrage_constrained" in result
    assert "noise_trader_risk" in result
    assert "constraint_index" in result
    assert 0 <= result["constraint_index"] <= 1
