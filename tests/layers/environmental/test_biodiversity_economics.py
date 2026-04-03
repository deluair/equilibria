import pytest
from app.layers.environmental.biodiversity_economics import BiodiversityEconomics


def test_instantiation():
    assert BiodiversityEconomics() is not None


def test_layer_id():
    assert BiodiversityEconomics.layer_id == "l9"


def test_name():
    assert BiodiversityEconomics().name == "Biodiversity Economics"


async def test_compute_empty_db_unavailable(db_conn):
    result = await BiodiversityEconomics().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


async def test_compute_with_land_data(db_conn):
    for sid, sname in [
        ("AG.LND.FRST.ZS", "Forest %"),
        ("AG.LND.FRST.K2", "Forest km2"),
        ("AG.LND.TOTL.K2", "Land km2"),
        ("NY.GDP.MKTP.KD", "GDP"),
        ("ER.PTD.TOTL.ZS", "Protected areas %"),
    ]:
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?,?,?,?)",
            ("wdi", sid, "BGD", sname),
        )
    values_map = {
        "AG.LND.FRST.ZS": 11.0,
        "AG.LND.FRST.K2": 14000.0,
        "AG.LND.TOTL.K2": 130000.0,
        "NY.GDP.MKTP.KD": 3e11,
        "ER.PTD.TOTL.ZS": 4.5,
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

    result = await BiodiversityEconomics().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


def test_species_area_zero_loss_when_stable():
    forest_ts = {str(yr): 30.0 for yr in range(2000, 2010)}
    result = BiodiversityEconomics._species_area(forest_ts, z=0.25)
    assert result["cumulative_species_loss_pct"] == pytest.approx(0.0)
    assert result["area_loss_pct"] == pytest.approx(0.0)


def test_species_area_habitat_loss_increases_species_loss():
    forest_ts = {str(yr): 30.0 - yr * 0.5 for yr in range(20)}
    result = BiodiversityEconomics._species_area(forest_ts, z=0.25)
    assert result["cumulative_species_loss_pct"] > 0


def test_teeb_valuation_includes_all_services():
    m = BiodiversityEconomics()
    result = m._teeb_valuation(
        ecosystem_type="mangrove",
        forest_km2_ts={"2020": 5000.0},
        land_km2_ts={"2020": 130000.0},
        gdp_ts={"2020": 3e11},
    )
    assert result["total_annual_value_musd"] > 0
    for svc in ("provisioning", "regulating", "cultural", "supporting"):
        assert svc in result["value_breakdown_usd_ha"]


def test_latest_value_returns_most_recent():
    ts = {"2018": 10.0, "2020": 30.0, "2019": 20.0}
    assert BiodiversityEconomics._latest_value(ts) == 30.0


def test_latest_value_empty_returns_none():
    assert BiodiversityEconomics._latest_value({}) is None
