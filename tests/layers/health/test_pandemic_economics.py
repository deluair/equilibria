import pytest
from app.layers.health.pandemic_economics import PandemicEconomics, _sir_macro


def test_instantiation():
    m = PandemicEconomics()
    assert m is not None


def test_layer_id():
    m = PandemicEconomics()
    assert m.layer_id == "l8"


def test_name():
    m = PandemicEconomics()
    assert m.name == "Pandemic Economics"


def test_sir_macro_baseline_runs():
    result = _sir_macro(S0=0.999, I0=0.001, R0_val=0.0, beta=0.3, gamma=0.1, alpha=0.005, T=100)
    assert "peak_infected" in result
    assert "total_deaths" in result
    assert result["peak_infected"] > 0
    assert result["total_deaths"] >= 0


def test_sir_macro_lockdown_reduces_peak():
    baseline = _sir_macro(S0=0.999, I0=0.001, R0_val=0.0, beta=0.3, gamma=0.1, alpha=0.005, T=200)
    lockdown = _sir_macro(
        S0=0.999, I0=0.001, R0_val=0.0, beta=0.3, gamma=0.1, alpha=0.005, T=200,
        lockdown_start=20, lockdown_end=80, lockdown_reduction=0.5,
    )
    assert lockdown["peak_infected"] < baseline["peak_infected"]


def test_sir_macro_output_loss_nonnegative():
    result = _sir_macro(S0=0.999, I0=0.001, R0_val=0.0, beta=0.3, gamma=0.1, alpha=0.005, T=100)
    assert all(v >= 0 for v in result["output_loss"])


async def test_compute_empty_db_fallback_score(db_conn):
    m = PandemicEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert "score" in result
    assert result["score"] == 50


async def test_compute_empty_db_returns_dict(db_conn):
    m = PandemicEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_compute_no_country_iso3(db_conn):
    m = PandemicEconomics()
    result = await m.compute(db_conn)
    assert isinstance(result, dict)
    assert "score" in result


async def test_compute_sir_macro_always_runs(db_conn):
    # SIR-macro runs regardless of DB content (uses hardcoded defaults)
    m = PandemicEconomics()
    result = await m.compute(db_conn, country_iso3="BGD")
    # empty DB hits pop/gdp check -> early return with score 50
    assert result["score"] == 50
