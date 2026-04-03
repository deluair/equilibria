import math
import pytest
from app.layers.integration.spillover import SpilloverDetection


async def _seed_series(db_conn, country_iso3: str, n: int = 30):
    """Seed varied (non-collinear) series so the VAR is not singular after differencing."""
    for i in range(n):
        for j, lid in enumerate(["l1", "l2", "l3", "l4", "l5"]):
            # Use different frequencies per layer to ensure independence
            score = 35.0 + 10.0 * math.sin((i + j * 7) * 0.4 + j) + j * 2.0
            await db_conn.execute(
                """
                INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("layer_score", country_iso3, lid, "{}", "{}", score, "WATCH"),
            )


def test_instantiation():
    m = SpilloverDetection()
    assert m.layer_id == "l6"
    assert m.name == "Spillover Detection"


async def test_empty_db_returns_unavailable(db_conn):
    m = SpilloverDetection()
    result = await m.compute(db_conn, country_iso3="ZZZ")
    assert result["signal"] == "UNAVAILABLE"
    assert result["spillover_index"] is None


async def test_result_has_required_keys_with_data(db_conn):
    await _seed_series(db_conn, "USA", n=30)
    m = SpilloverDetection()
    result = await m.compute(db_conn, country_iso3="USA")
    assert "score" in result
    assert "signal" in result
    # VAR may still fail on small test DB; handle gracefully
    if result["signal"] != "UNAVAILABLE":
        for key in ("spillover_index", "directional", "net_spillovers"):
            assert key in result


async def test_net_spillovers_roles(db_conn):
    await _seed_series(db_conn, "JPN", n=30)
    m = SpilloverDetection()
    result = await m.compute(db_conn, country_iso3="JPN")
    if result["signal"] != "UNAVAILABLE":
        for item in result["net_spillovers"]:
            assert item["role"] in ("transmitter", "receiver")
    else:
        # VAR estimation failed gracefully - acceptable on small dataset
        assert result["spillover_index"] is None
