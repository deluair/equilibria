import pytest
from app.layers.integration.cross_correlation import CrossLayerCorrelation
from tests.layers.integration.conftest import seed_layer_scores


async def _seed_series(db_conn, country_iso3: str, n: int = 15):
    """Insert n rows for each of l1-l5 with slightly varying scores."""
    for i in range(n):
        for j, lid in enumerate(["l1", "l2", "l3", "l4", "l5"]):
            score = 30.0 + j * 5.0 + i * 0.5
            signal = "WATCH" if score < 50 else "STRESS"
            await db_conn.execute(
                """
                INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("layer_score", country_iso3, lid, "{}", "{}", score, signal),
            )


def test_instantiation():
    m = CrossLayerCorrelation()
    assert m.layer_id == "l6"
    assert m.name == "Cross-Layer Correlation"


async def test_empty_db_returns_unavailable(db_conn):
    m = CrossLayerCorrelation()
    result = await m.compute(db_conn, country_iso3="ZZZ")
    assert result["signal"] == "UNAVAILABLE"
    assert result["correlation_matrix"] == {}


async def test_score_is_float_with_sufficient_data(db_conn):
    await _seed_series(db_conn, "USA", n=12)
    m = CrossLayerCorrelation()
    result = await m.compute(db_conn, country_iso3="USA")
    assert isinstance(result["score"], float)
    assert 0.0 <= result["score"] <= 100.0


async def test_correlation_matrix_structure(db_conn):
    await _seed_series(db_conn, "DEU", n=12)
    m = CrossLayerCorrelation()
    result = await m.compute(db_conn, country_iso3="DEU")
    matrix = result["correlation_matrix"]
    assert isinstance(matrix, dict)
    for lid, row in matrix.items():
        # Diagonal should be 1.0
        assert row[lid] == 1.0
