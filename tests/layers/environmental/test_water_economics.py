import pytest
from app.layers.environmental.water_economics import WaterEconomics


def test_instantiation():
    assert WaterEconomics() is not None


def test_layer_id():
    assert WaterEconomics.layer_id == "l9"


def test_name():
    assert WaterEconomics().name == "Water Economics"


async def test_compute_empty_db_unavailable(db_conn):
    result = await WaterEconomics().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


async def test_compute_with_water_data(db_conn):
    for sid, sname in [
        ("ER.H2O.INTR.PC", "FW per capita"),
        ("ER.H2O.FWTL.ZS", "Withdrawal %"),
        ("ER.H2O.FWAG.ZS", "Ag water %"),
        ("NY.GDP.MKTP.KD", "GDP"),
        ("SP.POP.TOTL", "Population"),
        ("AG.LND.IRIG.AG.ZS", "Irrigated %"),
    ]:
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?,?,?,?)",
            ("wdi", sid, "BGD", sname),
        )
    values_map = {
        "ER.H2O.INTR.PC": 8000.0,
        "ER.H2O.FWTL.ZS": 30.0,
        "ER.H2O.FWAG.ZS": 80.0,
        "NY.GDP.MKTP.KD": 3e11,
        "SP.POP.TOTL": 1.6e8,
        "AG.LND.IRIG.AG.ZS": 50.0,
    }
    for sid, base_val in values_map.items():
        row_id = (await db_conn.fetch_one(
            "SELECT id FROM data_series WHERE series_id=? AND country_iso3='BGD'", (sid,)
        ))["id"]
        for i in range(6):
            yr = 2015 + i
            await db_conn.execute(
                "INSERT INTO data_points (series_id, date, value) VALUES (?,?,?)",
                (row_id, f"{yr}-01-01", base_val),
            )

    result = await WaterEconomics().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


def test_water_scarcity_status_abundant():
    m = WaterEconomics()
    result = m._water_scarcity(
        fw_pc_ts={"2020": 5000.0},
        withdrawal_pct_ts={"2020": 10.0},
        discount_rate=0.05,
    )
    assert result["status"] == "abundant"
    assert result["freshwater_per_capita_m3"] == 5000


def test_water_scarcity_status_absolute_scarcity():
    m = WaterEconomics()
    result = m._water_scarcity(
        fw_pc_ts={"2020": 300.0},
        withdrawal_pct_ts={"2020": 90.0},
        discount_rate=0.05,
    )
    assert result["status"] == "absolute_scarcity"


def test_virtual_water_trade_net_exporter_low_income():
    m = WaterEconomics()
    result = m._virtual_water_trade(
        country="BGD",
        gdp_ts={"2020": 3e11},
        pop_ts={"2020": 1.6e8},
    )
    # GDP/capita ~1875 -> net exporter
    assert result["direction"] == "net_exporter"
    assert result["net_vw_import_per_capita_m3"] < 0


def test_groundwater_externalities_no_overexploitation_when_low():
    m = WaterEconomics()
    result = m._groundwater_externalities(
        withdrawal_pct_ts={"2020": 20.0},
        ag_water_ts={"2020": 70.0},
        gdp_ts={"2020": 3e11},
    )
    assert result["overexploitation"] is False
    assert result["pumping_cost_increase_pct"] == 0.0
