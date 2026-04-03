"""Unit tests for real_estate layer modules."""
import pytest

VALID_SIGNALS = ("UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS")


@pytest.mark.asyncio
async def test_housing_affordability_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.real_estate.housing_affordability import HousingAffordability
    db = await get_db()
    try:
        result = await HousingAffordability().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_real_estate_bubble_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.real_estate.real_estate_bubble import RealEstateBubble
    db = await get_db()
    try:
        result = await RealEstateBubble().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_mortgage_market_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.real_estate.mortgage_market import MortgageMarket
    db = await get_db()
    try:
        result = await MortgageMarket().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_housing_supply_constraints_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.real_estate.housing_supply_constraints import HousingSupplyConstraints
    db = await get_db()
    try:
        result = await HousingSupplyConstraints().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_construction_economics_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.real_estate.construction_economics import ConstructionEconomics
    db = await get_db()
    try:
        result = await ConstructionEconomics().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_land_value_taxation_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.real_estate.land_value_taxation import LandValueTaxation
    db = await get_db()
    try:
        result = await LandValueTaxation().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_urban_land_use_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.real_estate.urban_land_use import UrbanLandUse
    db = await get_db()
    try:
        result = await UrbanLandUse().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_commercial_real_estate_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.real_estate.commercial_real_estate import CommercialRealEstate
    db = await get_db()
    try:
        result = await CommercialRealEstate().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_housing_price_index_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.real_estate.housing_price_index import HousingPriceIndex
    db = await get_db()
    try:
        result = await HousingPriceIndex().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)


@pytest.mark.asyncio
async def test_rental_market_returns_valid(tmp_db):
    from app.db import get_db, release_db
    from app.layers.real_estate.rental_market import RentalMarket
    db = await get_db()
    try:
        result = await RentalMarket().run(db)
        assert isinstance(result, dict)
        assert "signal" in result
        assert result["signal"] in VALID_SIGNALS
        assert result.get("score") is None or isinstance(result["score"], (int, float))
    finally:
        await release_db(db)
