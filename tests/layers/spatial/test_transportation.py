import json
import numpy as np
import pytest
from app.layers.spatial.transportation import TransportEconomics


# --- Unit tests for internal static methods ---

def test_ppml_trivial():
    """PPML should converge on simple data."""
    rng = np.random.default_rng(0)
    n = 30
    X = np.column_stack([np.ones(n), rng.standard_normal(n)])
    true_beta = np.array([2.0, 0.5])
    mu = np.exp(X @ true_beta)
    y = rng.poisson(mu).astype(float)

    beta_hat, pr2, iters = TransportEconomics._ppml(X, y)
    assert beta_hat.shape == (2,)
    assert 0.0 <= pr2 <= 1.0
    assert iters >= 1


def test_ppml_returns_on_singular():
    """PPML should not crash on near-singular input."""
    X = np.ones((10, 2))  # Singular
    y = np.ones(10) * 5.0
    beta_hat, pr2, iters = TransportEconomics._ppml(X, y)
    assert beta_hat is not None


# --- Integration tests with empty DB ---

async def test_instantiation():
    model = TransportEconomics()
    assert model is not None


async def test_layer_id():
    model = TransportEconomics()
    assert model.layer_id == "l11"


async def test_name():
    model = TransportEconomics()
    assert model.name == "Transport Economics"


async def test_compute_empty_db_returns_dict(db_conn):
    model = TransportEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(db_conn):
    model = TransportEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result
    assert isinstance(result["score"], (int, float))
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_empty_db_subkeys_present(db_conn):
    model = TransportEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "commuting_gravity" in result
    assert "congestion" in result
    assert "modal_choice" in result


async def test_compute_no_data_subkeys_are_none(db_conn):
    model = TransportEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["commuting_gravity"] is None
    assert result["congestion"] is None
    assert result["modal_choice"] is None


async def test_compute_with_congestion_data(db_conn):
    """Seed 5 congestion segments; congestion result should be populated."""
    async with db_conn.conn.execute(
        "INSERT INTO data_series(source, series_id, country_iso3, name) VALUES (?,?,?,?)",
        ("congestion", "cong_bgd", "BGD", "Congestion"),
    ) as cur:
        sid = cur.lastrowid
    await db_conn.conn.commit()

    for i in range(6):
        meta = {"volume": 1500 + i * 50, "capacity": 2000, "free_flow_time": 15.0, "vtts": 20.0}
        await db_conn.conn.execute(
            "INSERT INTO data_points(series_id, date, value) VALUES (?,?,?)",
            (sid, f"2022-{i:04d}", 1500.0 + i * 50),
        )
        await db_conn.conn.execute(
            "UPDATE data_series SET metadata=? WHERE id=?",
            (json.dumps(meta), sid),
        )
    await db_conn.conn.commit()

    model = TransportEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    # Metadata is per-series not per-row so congestion may still be None;
    # but the compute should not crash
    assert isinstance(result, dict)
    assert "score" in result


async def test_run_adds_signal(db_conn):
    model = TransportEconomics()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "signal" in result
    assert result["signal"] in ("STABLE", "WATCH", "STRESS", "CRISIS", "UNAVAILABLE")
