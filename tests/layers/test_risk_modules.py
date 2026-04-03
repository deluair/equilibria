"""Unit tests for risk layer modules."""
import pytest


@pytest.mark.asyncio
async def test_country_risk_index_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.risk.country_risk_index import CountryRiskIndex
    db = await get_db()
    try:
        result = await CountryRiskIndex().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_sovereign_default_risk_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.risk.sovereign_default_risk import SovereignDefaultRisk
    db = await get_db()
    try:
        result = await SovereignDefaultRisk().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_tail_risk_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.risk.tail_risk import TailRisk
    db = await get_db()
    try:
        result = await TailRisk().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_var_cvar_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.risk.var_cvar import VaRCVaR
    db = await get_db()
    try:
        result = await VaRCVaR().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_political_risk_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.risk.political_risk import PoliticalRisk
    db = await get_db()
    try:
        result = await PoliticalRisk().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_commodity_risk_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.risk.commodity_risk import CommodityRisk
    db = await get_db()
    try:
        result = await CommodityRisk().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_currency_crisis_risk_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.risk.currency_crisis_risk import CurrencyCrisisRisk
    db = await get_db()
    try:
        result = await CurrencyCrisisRisk().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_contagion_model_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.risk.contagion_model import ContagionModel
    db = await get_db()
    try:
        result = await ContagionModel().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_macro_volatility_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.risk.macro_volatility import MacroVolatility
    db = await get_db()
    try:
        result = await MacroVolatility().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_global_risk_appetite_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.risk.global_risk_appetite import GlobalRiskAppetite
    db = await get_db()
    try:
        result = await GlobalRiskAppetite().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)
