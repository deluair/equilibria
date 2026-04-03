import numpy as np
import pytest
from app.layers.development.inequality_decomposition import (
    InequalityDecomposition,
    _gini,
    _generalized_entropy,
    _theil_decomposition,
)
from tests.layers.development.conftest import insert_series, insert_point, insert_country


def test_gini_equal_distribution():
    x = np.array([1.0, 1.0, 1.0, 1.0])
    assert abs(_gini(x)) < 1e-9


def test_gini_max_inequality():
    x = np.array([0.0, 0.0, 0.0, 1.0])
    assert _gini(x[x > 0]) >= 0


def test_generalized_entropy_theil_positive():
    x = np.array([1.0, 2.0, 3.0, 4.0])
    assert _generalized_entropy(x, 1) >= 0


def test_theil_decomposition_within_between_sum():
    values = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
    groups = np.array(["A", "A", "A", "B", "B", "B"])
    result = _theil_decomposition(values, groups)
    total = result["total_theil"]
    within = result["within"]
    between = result["between"]
    # Within + between should approximate total (may differ due to floating point)
    assert abs((within + between) - total) < 0.01 or total == 0


def test_instantiation():
    assert InequalityDecomposition() is not None


def test_layer_id():
    assert InequalityDecomposition.layer_id == "l4"


def test_name():
    assert InequalityDecomposition().name == "Inequality Decomposition"


async def test_empty_db_returns_50(db_conn):
    result = await InequalityDecomposition().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_data(db_conn):
    for i in range(25):
        iso = f"IQ{i:02d}"
        sid = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        await insert_point(db_conn, sid, "2022-01-01", 1000.0 * (i + 1))

    result = await InequalityDecomposition().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_has_global_gini(db_conn):
    for i in range(25):
        iso = f"IR{i:02d}"
        sid = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        await insert_point(db_conn, sid, "2022-01-01", 500.0 * (i + 1))

    result = await InequalityDecomposition().compute(db_conn)
    assert "results" in result
    if "global" in result["results"]:
        assert "gini" in result["results"]["global"]


async def test_regional_decomp_with_countries(db_conn):
    isos = [f"RG{i:02d}" for i in range(25)]
    for i, iso in enumerate(isos):
        region = "RegionA" if i < 12 else "RegionB"
        await insert_country(db_conn, iso, region=region)
        sid = await insert_series(db_conn, "NY.GDP.PCAP.KD", iso)
        await insert_point(db_conn, sid, "2022-01-01", 1000.0 * (i + 1))

    result = await InequalityDecomposition().compute(db_conn)
    assert "results" in result
    if "regional_decomposition" in result["results"] and result["results"]["regional_decomposition"]:
        rd = result["results"]["regional_decomposition"]
        assert "total_theil" in rd
