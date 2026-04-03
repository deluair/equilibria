import pytest
from app.layers.integration.signal_classifier import SignalClassifier
from tests.layers.integration.conftest import seed_composite_scores


def test_instantiation():
    m = SignalClassifier()
    assert m.layer_id == "l6"
    assert m.name == "Signal Classifier"


async def test_insufficient_history_returns_unavailable(db_conn):
    m = SignalClassifier()
    result = await m.compute(db_conn, country_iso3="ZZZ")
    assert result["signal"] == "UNAVAILABLE"
    assert result["score"] is None


async def test_classify_trend_flat():
    import numpy as np
    m = SignalClassifier()
    scores = np.full(10, 30.0)
    slope, trend = m._classify_trend(scores)
    assert trend == "FLAT"


async def test_full_result_structure_with_history(db_conn):
    await seed_composite_scores(db_conn, country_iso3="USA", n=20, base=30.0)
    m = SignalClassifier()
    result = await m.compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)
    assert "signal" in result
    assert result["signal"] in ("STABLE", "WATCH", "STRESS", "CRISIS")
    assert "metrics" in result
    assert "trend_slope" in result["metrics"]
    assert "volatility" in result["metrics"]
