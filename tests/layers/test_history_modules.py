"""Unit tests for history layer modules."""
import pytest


@pytest.mark.asyncio
async def test_long_run_growth_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.history.long_run_growth import LongRunGrowth
    db = await get_db()
    try:
        result = await LongRunGrowth().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_colonial_legacy_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.history.colonial_legacy import ColonialLegacy
    db = await get_db()
    try:
        result = await ColonialLegacy().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_institutional_persistence_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.history.institutional_persistence import InstitutionalPersistence
    db = await get_db()
    try:
        result = await InstitutionalPersistence().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_historical_inequality_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.history.historical_inequality import HistoricalInequality
    db = await get_db()
    try:
        result = await HistoricalInequality().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_war_economic_cost_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.history.war_economic_cost import WarEconomicCost
    db = await get_db()
    try:
        result = await WarEconomicCost().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_technological_diffusion_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.history.technological_diffusion import TechnologicalDiffusion
    db = await get_db()
    try:
        result = await TechnologicalDiffusion().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_great_depression_analogy_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.history.great_depression_analogy import GreatDepressionAnalogy
    db = await get_db()
    try:
        result = await GreatDepressionAnalogy().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_historical_trade_patterns_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.history.historical_trade_patterns import HistoricalTradePatterns
    db = await get_db()
    try:
        result = await HistoricalTradePatterns().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_demographic_transition_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.history.demographic_transition import DemographicTransition
    db = await get_db()
    try:
        result = await DemographicTransition().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_economic_revolutions_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.history.economic_revolutions import EconomicRevolutions
    db = await get_db()
    try:
        result = await EconomicRevolutions().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)
