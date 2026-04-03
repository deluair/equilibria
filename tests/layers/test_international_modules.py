"""Unit tests for international layer modules."""
import pytest


@pytest.mark.asyncio
async def test_foreign_aid_effectiveness_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.international.foreign_aid_effectiveness import ForeignAidEffectiveness
    db = await get_db()
    try:
        result = await ForeignAidEffectiveness().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_dollar_dominance_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.international.dollar_dominance import DollarDominance
    db = await get_db()
    try:
        result = await DollarDominance().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_global_imbalances_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.international.global_imbalances import GlobalImbalances
    db = await get_db()
    try:
        result = await GlobalImbalances().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_geopolitical_risk_index_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.international.geopolitical_risk_index import GeopoliticalRiskIndex
    db = await get_db()
    try:
        result = await GeopoliticalRiskIndex().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_diplomatic_trade_links_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.international.diplomatic_trade_links import DiplomaticTradeLinks
    db = await get_db()
    try:
        result = await DiplomaticTradeLinks().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_sanctions_impact_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.international.sanctions_impact import SanctionsImpact
    db = await get_db()
    try:
        result = await SanctionsImpact().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_regional_integration_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.international.regional_integration import RegionalIntegration
    db = await get_db()
    try:
        result = await RegionalIntegration().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_currency_wars_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.international.currency_wars import CurrencyWars
    db = await get_db()
    try:
        result = await CurrencyWars().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_multilateral_negotiations_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.international.multilateral_negotiations import MultilateralNegotiations
    db = await get_db()
    try:
        result = await MultilateralNegotiations().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_imf_program_effects_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.international.imf_program_effects import IMFProgramEffects
    db = await get_db()
    try:
        result = await IMFProgramEffects().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)
