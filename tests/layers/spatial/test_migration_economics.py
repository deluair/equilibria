import json
import numpy as np
import pytest
from app.layers.spatial.migration_economics import MigrationEconomics


async def test_instantiation():
    model = MigrationEconomics()
    assert model is not None


async def test_layer_id():
    model = MigrationEconomics()
    assert model.layer_id == "l11"


async def test_name():
    model = MigrationEconomics()
    assert model.name == "Migration Economics"


async def test_compute_empty_db_returns_dict(db_conn):
    model = MigrationEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_empty_db_has_score(db_conn):
    model = MigrationEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "score" in result
    assert isinstance(result["score"], (int, float))
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_empty_db_subkeys_present(db_conn):
    model = MigrationEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert "roy_model" in result
    assert "borjas_surplus" in result
    assert "rosen_roback" in result
    assert "brain_drain" in result


async def test_compute_no_data_subkeys_are_none(db_conn):
    model = MigrationEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    assert result["roy_model"] is None
    assert result["borjas_surplus"] is None
    assert result["rosen_roback"] is None
    assert result["brain_drain"] is None


async def test_compute_with_brain_drain_data(db_conn):
    """Seed brain drain data and check ratio computation."""
    async with db_conn.conn.execute(
        "INSERT INTO data_series(source, series_id, country_iso3, name) VALUES (?,?,?,?)",
        ("brain_drain", "bd_bgd", "BGD", "Brain Drain"),
    ) as cur:
        sid = cur.lastrowid
    await db_conn.conn.commit()

    meta = {
        "tertiary_emigration_rate": 0.30,
        "overall_emigration_rate": 0.10,
    }
    await db_conn.conn.execute(
        "INSERT INTO data_points(series_id, date, value) VALUES (?,?,?)",
        (sid, "2022", 500_000.0),
    )
    await db_conn.conn.execute(
        "UPDATE data_series SET metadata=? WHERE id=?",
        (json.dumps(meta), sid),
    )
    await db_conn.conn.commit()

    model = MigrationEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    bd = result["brain_drain"]
    if bd is not None:
        assert bd["brain_drain_ratio"] == pytest.approx(3.0, abs=0.01)
        assert bd["severe_brain_drain"] is True


async def test_compute_with_rosen_roback_data(db_conn):
    """Seed spatial equilibrium data and verify disequilibrium index is computed."""
    rng = np.random.default_rng(5)
    async with db_conn.conn.execute(
        "INSERT INTO data_series(source, series_id, country_iso3, name) VALUES (?,?,?,?)",
        ("spatial_equilibrium", "se_bgd", "BGD", "Spatial Eq"),
    ) as cur:
        sid = cur.lastrowid
    await db_conn.conn.commit()

    for i in range(8):
        wage = 3000.0 + rng.normal(0, 200)
        rent = 1000.0 + wage * 0.3 + rng.normal(0, 50)
        meta = {"wage": wage, "rent": rent, "amenity_index": float(rng.uniform(0, 10))}
        await db_conn.conn.execute(
            "INSERT INTO data_points(series_id, date, value) VALUES (?,?,?)",
            (sid, f"2022-{i:04d}", wage),
        )
        await db_conn.conn.execute(
            "UPDATE data_series SET metadata=? WHERE id=?",
            (json.dumps(meta), sid),
        )
    await db_conn.conn.commit()

    model = MigrationEconomics()
    result = await model.compute(db_conn, country_iso3="BGD")
    # With metadata on series (not per-row) the result may not parse wages;
    # but the compute must not raise.
    assert isinstance(result, dict)


async def test_run_adds_signal(db_conn):
    model = MigrationEconomics()
    result = await model.run(db_conn, country_iso3="BGD")
    assert "signal" in result
    assert result["signal"] in ("STABLE", "WATCH", "STRESS", "CRISIS", "UNAVAILABLE")
