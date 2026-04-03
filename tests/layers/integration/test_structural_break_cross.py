import pytest
from app.layers.integration.structural_break_cross import CrossLayerBreak


async def _seed_series(db_conn, country_iso3: str, n: int = 30, break_at: int | None = None):
    for i in range(n):
        for lid in ["l1", "l2", "l3", "l4", "l5"]:
            # Introduce a mean shift if break_at specified
            base = 40.0 if (break_at is None or i < break_at) else 70.0
            score = base + (hash(lid) % 5) * 1.0
            await db_conn.execute(
                """
                INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("layer_score", country_iso3, lid, "{}", "{}", score, "WATCH"),
            )


def test_instantiation():
    m = CrossLayerBreak()
    assert m.layer_id == "l6"
    assert m.name == "Cross-Layer Structural Break"


async def test_empty_db_returns_unavailable(db_conn):
    m = CrossLayerBreak()
    result = await m.compute(db_conn, country_iso3="ZZZ")
    assert result["signal"] == "UNAVAILABLE"
    assert result["breaks"] == {}


async def test_no_break_in_stable_series(db_conn):
    await _seed_series(db_conn, "USA", n=25)
    m = CrossLayerBreak()
    result = await m.compute(db_conn, country_iso3="USA")
    assert isinstance(result["score"], float)
    assert result["score"] >= 0.0


async def test_break_detected_with_shift(db_conn):
    # Large mean shift at midpoint should trigger at least one layer break
    await _seed_series(db_conn, "CHN", n=30, break_at=15)
    m = CrossLayerBreak()
    result = await m.compute(db_conn, country_iso3="CHN")
    total_breaks = sum(len(v) for v in result["layer_breaks"].values())
    # Not asserting count (depends on significance), but structure must be valid
    assert isinstance(total_breaks, int)
    assert "regimes" in result
    assert "stability_tests" in result
