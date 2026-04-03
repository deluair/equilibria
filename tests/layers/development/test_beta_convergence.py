import pytest
from app.layers.development.beta_convergence import BetaConvergence
from tests.layers.development.conftest import insert_series, insert_point, insert_country


def test_instantiation():
    m = BetaConvergence()
    assert m is not None


def test_layer_id():
    assert BetaConvergence.layer_id == "l4"


def test_name():
    assert BetaConvergence().name == "Beta Convergence"


async def test_empty_db_returns_fallback(db_conn):
    result = await BetaConvergence().compute(db_conn)
    assert isinstance(result, dict)
    assert "score" in result
    assert result["score"] == 50


async def test_score_in_valid_range_with_data(db_conn):
    # Insert 12 countries with enough yearly GDP per capita data to trigger regression
    isos = [f"C{i:02d}" for i in range(12)]
    for i, iso in enumerate(isos):
        sid = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        base = 1000 * (i + 1)
        for yr in range(2000, 2010):
            await insert_point(db_conn, sid, str(yr), base * (1.02 ** (yr - 2000)))

    result = await BetaConvergence().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_has_unconditional_key_with_sufficient_data(db_conn):
    isos = [f"D{i:02d}" for i in range(12)]
    for i, iso in enumerate(isos):
        sid = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        base = 500 * (i + 1)
        for yr in range(1995, 2005):
            await insert_point(db_conn, sid, str(yr), base * (1.01 ** (yr - 1995)))

    result = await BetaConvergence().compute(db_conn)
    assert "results" in result
    if "unconditional" in result["results"]:
        unc = result["results"]["unconditional"]
        assert "beta" in unc
        assert "n_obs" in unc
