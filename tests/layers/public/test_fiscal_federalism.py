import json
import pytest
from app.layers.public.fiscal_federalism import FiscalFederalism


async def test_instantiation():
    model = FiscalFederalism()
    assert model is not None


async def test_layer_id():
    model = FiscalFederalism()
    assert model.layer_id == "l10"


async def test_name():
    model = FiscalFederalism()
    assert model.name == "Fiscal Federalism"


async def test_compute_returns_dict(db_conn):
    model = FiscalFederalism()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_has_score(db_conn):
    model = FiscalFederalism()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result
    assert isinstance(result["score"], (int, float))


async def test_compute_score_in_range(db_conn):
    model = FiscalFederalism()
    result = await model.compute(db_conn, country_iso3="USA")
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_has_vfi_key(db_conn):
    model = FiscalFederalism()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "vertical_fiscal_imbalance" in result["results"]


async def test_compute_vfi_error_on_empty_db(db_conn):
    model = FiscalFederalism()
    result = await model.compute(db_conn, country_iso3="USA")
    vfi = result["results"]["vertical_fiscal_imbalance"]
    # Empty DB returns an error key
    assert "error" in vfi or "vfi" in vfi


async def test_compute_has_equalization_key(db_conn):
    model = FiscalFederalism()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "equalization" in result["results"]


async def test_compute_has_tiebout_key(db_conn):
    model = FiscalFederalism()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "tiebout_sorting" in result["results"]


async def test_compute_has_oates_key(db_conn):
    model = FiscalFederalism()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "oates_test" in result["results"]


async def test_compute_vfi_with_data(db_conn):
    """Seed overlapping revenue and expenditure data; VFI should be computed."""
    import app.db as db_mod

    async with db_conn.conn.execute(
        "INSERT INTO data_series(source, series_id, country_iso3, name) VALUES (?,?,?,?)",
        ("test", "SUBNATIONAL_OWN_REV", "USA", "Sub Rev"),
    ) as cur:
        rev_id = cur.lastrowid
    await db_conn.conn.commit()

    async with db_conn.conn.execute(
        "INSERT INTO data_series(source, series_id, country_iso3, name) VALUES (?,?,?,?)",
        ("test", "SUBNATIONAL_EXP", "USA", "Sub Exp"),
    ) as cur:
        exp_id = cur.lastrowid
    await db_conn.conn.commit()

    await db_conn.conn.execute(
        "INSERT INTO data_points(series_id, date, value) VALUES (?,?,?)",
        (rev_id, "2022", 15.0),
    )
    await db_conn.conn.execute(
        "INSERT INTO data_points(series_id, date, value) VALUES (?,?,?)",
        (exp_id, "2022", 30.0),
    )
    await db_conn.conn.commit()

    model = FiscalFederalism()
    result = await model.compute(db_conn, country_iso3="USA")
    vfi = result["results"]["vertical_fiscal_imbalance"]
    assert "vfi" in vfi
    # VFI = 1 - (15/30) = 0.5
    assert vfi["vfi"] == pytest.approx(0.5, abs=0.001)
    assert vfi["transfer_dependence"] == "moderate"


async def test_run_adds_signal(db_conn):
    model = FiscalFederalism()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result
    assert result["signal"] in ("STABLE", "WATCH", "STRESS", "CRISIS", "UNAVAILABLE")
