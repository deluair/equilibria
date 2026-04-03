"""Unit tests for welfare layer modules."""
import pytest

VALID_SIGNALS = ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")


@pytest.mark.asyncio
async def test_atkinson_inequality_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.welfare.atkinson_inequality import AtkinsonInequality
    db = await get_db()
    try:
        result = await AtkinsonInequality().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_poverty_decomposition_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.welfare.poverty_decomposition import PovertyDecomposition
    db = await get_db()
    try:
        result = await PovertyDecomposition().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_capabilities_index_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.welfare.capabilities_index import CapabilitiesIndex
    db = await get_db()
    try:
        result = await CapabilitiesIndex().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_subjective_wellbeing_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.welfare.subjective_wellbeing import SubjectiveWellbeing
    db = await get_db()
    try:
        result = await SubjectiveWellbeing().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_living_standards_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.welfare.living_standards import LivingStandards
    db = await get_db()
    try:
        result = await LivingStandards().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_redistribution_analysis_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.welfare.redistribution_analysis import RedistributionAnalysis
    db = await get_db()
    try:
        result = await RedistributionAnalysis().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_social_exclusion_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.welfare.social_exclusion import SocialExclusion
    db = await get_db()
    try:
        result = await SocialExclusion().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)
