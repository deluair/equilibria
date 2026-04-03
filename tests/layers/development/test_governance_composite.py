import pytest
from app.layers.development.governance_composite import GovernanceComposite, WGI_INDICATORS
from tests.layers.development.conftest import insert_series, insert_point


def test_instantiation():
    assert GovernanceComposite() is not None


def test_layer_id():
    assert GovernanceComposite.layer_id == "l4"


def test_name():
    assert GovernanceComposite().name == "Governance Composite"


def test_wgi_indicators_count():
    assert len(WGI_INDICATORS) == 6


async def test_empty_db_returns_50(db_conn):
    result = await GovernanceComposite().compute(db_conn)
    assert result["score"] == 50


async def test_score_in_range_with_full_wgi(db_conn):
    isos = [f"GC{i:02d}" for i in range(15)]
    wgi_series = [s for s, _ in WGI_INDICATORS]
    for i, iso in enumerate(isos):
        for series_id in wgi_series:
            sid = await insert_series(db_conn, series_id, iso)
            await insert_point(db_conn, sid, "2022-01-01", -2.0 + i * 0.28)

    result = await GovernanceComposite().compute(db_conn)
    assert 0 <= result["score"] <= 100


async def test_results_has_pca_loadings(db_conn):
    isos = [f"GD{i:02d}" for i in range(15)]
    wgi_series = [s for s, _ in WGI_INDICATORS]
    for i, iso in enumerate(isos):
        for series_id in wgi_series:
            sid = await insert_series(db_conn, series_id, iso)
            await insert_point(db_conn, sid, "2022-01-01", -1.5 + i * 0.2)

    result = await GovernanceComposite().compute(db_conn)
    assert "results" in result
    if "pca" in result["results"]:
        assert "loadings" in result["results"]["pca"]
        assert "variance_explained" in result["results"]["pca"]


async def test_target_cluster_key(db_conn):
    isos = [f"GE{i:02d}" for i in range(15)]
    wgi_series = [s for s, _ in WGI_INDICATORS]
    for i, iso in enumerate(isos):
        for series_id in wgi_series:
            sid = await insert_series(db_conn, series_id, iso)
            await insert_point(db_conn, sid, "2022-01-01", -1.0 + i * 0.15)

    result = await GovernanceComposite().compute(db_conn, country_iso3="GE00")
    assert result["results"]["country_iso3"] == "GE00"
    if "target_cluster" in result["results"] and result["results"]["target_cluster"]:
        assert result["results"]["target_cluster"] in ("weak", "moderate", "good", "strong")
