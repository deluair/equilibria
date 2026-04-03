import pytest
from app.layers.public.tax_incidence import TaxIncidence, _gini, _concentration_coefficient, _laffer_revenue


# --- Unit tests for pure functions ---

def test_gini_equal_distribution():
    import numpy as np
    values = np.array([1.0, 1.0, 1.0, 1.0])
    g = _gini(values)
    assert g == pytest.approx(0.0, abs=0.05)


def test_gini_maximum_inequality():
    import numpy as np
    values = np.array([0.0, 0.0, 0.0, 100.0])
    # With 4 elements and one nonzero, Gini is high
    g = _gini(values)
    assert g > 0.5


def test_laffer_revenue_zero_rate():
    rev = _laffer_revenue(0.0, 1.0, 0.4)
    assert rev == 0.0


def test_laffer_revenue_peak_at_t_star():
    # Revenue-maximizing rate: t* = 1/(1+e) = 1/1.4 ~ 0.714
    eps = 0.4
    t_star = 1.0 / (1.0 + eps)
    rev_star = _laffer_revenue(t_star, 1.0, eps)
    rev_below = _laffer_revenue(t_star - 0.05, 1.0, eps)
    rev_above = _laffer_revenue(t_star + 0.05, 1.0, eps)
    assert rev_star >= rev_below
    assert rev_star >= rev_above


def test_concentration_coefficient_sorted_ranks():
    import numpy as np
    values = np.array([10.0, 20.0, 30.0, 40.0])
    ranks = np.array([0, 1, 2, 3])
    cc = _concentration_coefficient(values, ranks)
    assert isinstance(cc, float)
    assert -1.0 <= cc <= 1.0


# --- Integration tests with empty DB ---

async def test_instantiation():
    model = TaxIncidence()
    assert model is not None


async def test_layer_id():
    model = TaxIncidence()
    assert model.layer_id == "l10"


async def test_name():
    model = TaxIncidence()
    assert model.name == "Tax Incidence"


async def test_compute_returns_dict(db_conn):
    model = TaxIncidence()
    result = await model.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_compute_has_score(db_conn):
    model = TaxIncidence()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "score" in result
    assert isinstance(result["score"], (int, float))


async def test_compute_score_in_range(db_conn):
    model = TaxIncidence()
    result = await model.compute(db_conn, country_iso3="USA")
    assert 0.0 <= result["score"] <= 100.0


async def test_compute_has_results_key(db_conn):
    model = TaxIncidence()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "results" in result


async def test_compute_harberger_present(db_conn):
    model = TaxIncidence()
    result = await model.compute(db_conn, country_iso3="USA")
    assert "harberger" in result["results"]
    h = result["results"]["harberger"]
    assert "capital_burden_share" in h
    assert "labor_burden_share" in h
    # Burden shares sum to 1
    assert h["capital_burden_share"] + h["labor_burden_share"] == pytest.approx(1.0, abs=0.001)


async def test_compute_vat_passthrough_present(db_conn):
    model = TaxIncidence()
    result = await model.compute(db_conn, country_iso3="USA")
    vat = result["results"]["vat_passthrough"]
    assert "consumer_share" in vat
    assert "producer_share" in vat
    assert vat["consumer_share"] + vat["producer_share"] == pytest.approx(1.0, abs=0.001)


async def test_compute_laffer_present(db_conn):
    model = TaxIncidence()
    result = await model.compute(db_conn, country_iso3="USA")
    laffer = result["results"]["laffer"]
    assert "revenue_maximizing_rate" in laffer
    assert 0.0 < laffer["revenue_maximizing_rate"] < 1.0


async def test_compute_custom_kwargs(db_conn):
    model = TaxIncidence()
    result = await model.compute(
        db_conn,
        country_iso3="USA",
        vat_rate=0.20,
        corp_tax_rate=0.25,
        taxable_income_elasticity=0.5,
    )
    assert result["results"]["vat_passthrough"]["vat_rate"] == 0.20
    assert result["results"]["harberger"]["corp_tax_rate"] == 0.25


async def test_run_adds_signal(db_conn):
    model = TaxIncidence()
    result = await model.run(db_conn, country_iso3="USA")
    assert "signal" in result
    assert result["signal"] in ("STABLE", "WATCH", "STRESS", "CRISIS", "UNAVAILABLE")
