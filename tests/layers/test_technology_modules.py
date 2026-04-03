"""Unit tests for technology layer modules."""
import pytest

VALID_SIGNALS = ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")


@pytest.mark.asyncio
async def test_tfp_estimation_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.technology.tfp_estimation import TFPEstimation
    db = await get_db()
    try:
        result = await TFPEstimation().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_innovation_index_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.technology.innovation_index import InnovationIndex
    db = await get_db()
    try:
        result = await InnovationIndex().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_digital_economy_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.technology.digital_economy import DigitalEconomy
    db = await get_db()
    try:
        result = await DigitalEconomy().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_automation_labor_impact_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.technology.automation_labor_impact import AutomationLaborImpact
    db = await get_db()
    try:
        result = await AutomationLaborImpact().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_technology_diffusion_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.technology.technology_diffusion import TechnologyDiffusion
    db = await get_db()
    try:
        result = await TechnologyDiffusion().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_rnd_returns_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.technology.rnd_returns import RnDReturns
    db = await get_db()
    try:
        result = await RnDReturns().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_knowledge_spillovers_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.technology.knowledge_spillovers import KnowledgeSpillovers
    db = await get_db()
    try:
        result = await KnowledgeSpillovers().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_network_effects_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.technology.network_effects import NetworkEffects
    db = await get_db()
    try:
        result = await NetworkEffects().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_platform_economics_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.technology.platform_economics import PlatformEconomics
    db = await get_db()
    try:
        result = await PlatformEconomics().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)
