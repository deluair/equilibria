import pytest
from app.layers.environmental.renewable_transition import RenewableTransition


def test_instantiation():
    assert RenewableTransition() is not None


def test_layer_id():
    assert RenewableTransition.layer_id == "l9"


def test_name():
    assert RenewableTransition().name == "Renewable Transition"


async def test_compute_empty_db_unavailable(db_conn):
    result = await RenewableTransition().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert result.get("signal") == "UNAVAILABLE" or result.get("score") is None


async def test_compute_with_energy_data(db_conn):
    for sid, sname in [
        ("EG.ELC.RNEW.ZS", "Renewable elec %"),
        ("EN.ATM.CO2E.KT", "CO2 kt"),
        ("NY.GDP.MKTP.KD", "GDP"),
    ]:
        await db_conn.execute(
            "INSERT INTO data_series (source, series_id, country_iso3, name) VALUES (?,?,?,?)",
            ("wdi", sid, "BGD", sname),
        )
    for sid, base_val in [
        ("EG.ELC.RNEW.ZS", 5.0),
        ("EN.ATM.CO2E.KT", 60000.0),
        ("NY.GDP.MKTP.KD", 3e11),
    ]:
        row_id = (await db_conn.fetch_one(
            "SELECT id FROM data_series WHERE series_id=? AND country_iso3='BGD'", (sid,)
        ))["id"]
        for i in range(6):
            yr = 2015 + i
            await db_conn.execute(
                "INSERT INTO data_points (series_id, date, value) VALUES (?,?,?)",
                (row_id, f"{yr}-01-01", base_val + i * 0.5),
            )

    result = await RenewableTransition().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)
    assert "score" in result
    if result["score"] is not None:
        assert 0 <= result["score"] <= 100


def test_compute_learning_curves_keys():
    m = RenewableTransition()
    lc = m._compute_learning_curves()
    assert "solar_pv" in lc
    assert "onshore_wind" in lc
    for tech, data in lc.items():
        assert "learning_rate" in data
        assert "cost_at_doublings" in data


def test_compute_lcoe_cheapest_is_renewable():
    m = RenewableTransition()
    lcoe = m._compute_lcoe(discount_rate=0.07)
    assert "cheapest" in lcoe
    assert lcoe["cheapest"] in m.LCOE_BENCHMARKS


def test_analyze_re_trajectory_projects_forward():
    ts = {str(yr): 10.0 + (yr - 2010) * 1.5 for yr in range(2010, 2022)}
    result = RenewableTransition._analyze_re_trajectory(ts)
    assert result["trend_pct_per_year"] > 0
    assert result["projected_year_50pct"] is not None
    assert result["projected_year_50pct"] > 2021


def test_estimate_stranded_assets_returns_fraction():
    co2_ts = {"2020": 60000.0}
    gdp_ts = {"2020": 3e11}
    result = RenewableTransition._estimate_stranded_assets(
        co2_ts=co2_ts, gdp_ts=gdp_ts,
        fossil_share_ts={"coal": {"2020": 30.0}, "gas": {"2020": 20.0}},
        carbon_budget_gtco2=500.0,
    )
    assert 0 <= result["stranded_fraction"] <= 1
    assert result["annual_co2_gt"] > 0
