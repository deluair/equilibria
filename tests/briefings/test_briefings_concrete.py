"""Tests for concrete briefing classes: EconomicConditions, TradeFlash, CountryDeepDive."""

import pytest

from app.briefings.country_deep_dive import CountryDeepDiveBriefing
from app.briefings.economic_conditions import EconomicConditionsBriefing
from app.briefings.trade_flash import TradeFlashBriefing


# ---------------------------------------------------------------------------
# EconomicConditionsBriefing
# ---------------------------------------------------------------------------

def test_economic_conditions_instantiation():
    """EconomicConditionsBriefing instantiates with correct type and cadence."""
    b = EconomicConditionsBriefing()
    assert b.briefing_type == "economic_conditions"
    assert b.cadence == "weekly"


def test_economic_conditions_has_data_sources():
    """EconomicConditionsBriefing sets non-empty data_sources."""
    b = EconomicConditionsBriefing()
    assert isinstance(b.data_sources, list)
    assert len(b.data_sources) > 0


def test_economic_conditions_has_methodology_note():
    """EconomicConditionsBriefing sets a methodology note."""
    b = EconomicConditionsBriefing()
    assert isinstance(b.methodology_note, str)
    assert len(b.methodology_note) > 20


def test_economic_conditions_title_template():
    """EconomicConditionsBriefing title_template contains {date} placeholder."""
    b = EconomicConditionsBriefing()
    assert "{date}" in b.title_template


@pytest.mark.asyncio
async def test_economic_conditions_generate_empty_db(tmp_db):
    """EconomicConditionsBriefing.generate() runs on an empty DB without error."""
    from app.db import get_db, release_db
    b = EconomicConditionsBriefing()
    db = await get_db()
    try:
        result = await b.generate(db)
    finally:
        await release_db(db)
    assert result["briefing_type"] == "economic_conditions"
    assert isinstance(result["body_html"], str)
    assert len(result["body_html"]) > 100


@pytest.mark.asyncio
async def test_economic_conditions_sections_built(tmp_db):
    """EconomicConditionsBriefing.generate() populates at least one section."""
    from app.db import get_db, release_db
    b = EconomicConditionsBriefing()
    db = await get_db()
    try:
        await b.generate(db)
    finally:
        await release_db(db)
    assert len(b.sections) >= 1


@pytest.mark.asyncio
async def test_economic_conditions_html_has_sections(tmp_db):
    """EconomicConditionsBriefing HTML includes Executive Summary heading."""
    from app.db import get_db, release_db
    b = EconomicConditionsBriefing()
    db = await get_db()
    try:
        result = await b.generate(db)
    finally:
        await release_db(db)
    assert "Executive Summary" in result["body_html"]


# ---------------------------------------------------------------------------
# TradeFlashBriefing
# ---------------------------------------------------------------------------

def test_trade_flash_instantiation():
    """TradeFlashBriefing instantiates with correct type and cadence."""
    b = TradeFlashBriefing()
    assert b.briefing_type == "trade_flash"
    assert b.cadence == "weekly"


def test_trade_flash_has_data_sources():
    """TradeFlashBriefing sets non-empty data_sources."""
    b = TradeFlashBriefing()
    assert len(b.data_sources) > 0


def test_trade_flash_has_methodology_note():
    """TradeFlashBriefing sets a methodology note."""
    b = TradeFlashBriefing()
    assert len(b.methodology_note) > 20


@pytest.mark.asyncio
async def test_trade_flash_generate_empty_db(tmp_db):
    """TradeFlashBriefing.generate() runs on an empty DB without error."""
    from app.db import get_db, release_db
    b = TradeFlashBriefing()
    db = await get_db()
    try:
        result = await b.generate(db)
    finally:
        await release_db(db)
    assert result["briefing_type"] == "trade_flash"
    assert isinstance(result["body_html"], str)
    assert len(result["body_html"]) > 100


@pytest.mark.asyncio
async def test_trade_flash_sections_built(tmp_db):
    """TradeFlashBriefing.generate() populates sections."""
    from app.db import get_db, release_db
    b = TradeFlashBriefing()
    db = await get_db()
    try:
        await b.generate(db)
    finally:
        await release_db(db)
    assert len(b.sections) >= 1


@pytest.mark.asyncio
async def test_trade_flash_html_has_executive_summary(tmp_db):
    """TradeFlashBriefing HTML includes Executive Summary section."""
    from app.db import get_db, release_db
    b = TradeFlashBriefing()
    db = await get_db()
    try:
        result = await b.generate(db)
    finally:
        await release_db(db)
    assert "Executive Summary" in result["body_html"]


# ---------------------------------------------------------------------------
# CountryDeepDiveBriefing
# ---------------------------------------------------------------------------

def test_country_deep_dive_instantiation_default():
    """CountryDeepDiveBriefing defaults to USA."""
    b = CountryDeepDiveBriefing()
    assert b.briefing_type == "country_deep_dive"
    assert b.country_iso3 == "USA"


def test_country_deep_dive_instantiation_with_country():
    """CountryDeepDiveBriefing accepts a custom country ISO3."""
    b = CountryDeepDiveBriefing(country_iso3="BGD")
    assert b.country_iso3 == "BGD"


def test_country_deep_dive_iso3_uppercased():
    """CountryDeepDiveBriefing uppercases ISO3 on init."""
    b = CountryDeepDiveBriefing(country_iso3="deu")
    assert b.country_iso3 == "DEU"


def test_country_deep_dive_cadence():
    """CountryDeepDiveBriefing cadence is on_demand."""
    b = CountryDeepDiveBriefing()
    assert b.cadence == "on_demand"


def test_country_deep_dive_has_methodology_note():
    """CountryDeepDiveBriefing sets a methodology note."""
    b = CountryDeepDiveBriefing()
    assert len(b.methodology_note) > 20


def test_country_deep_dive_has_data_sources():
    """CountryDeepDiveBriefing sets data_sources list."""
    b = CountryDeepDiveBriefing()
    assert len(b.data_sources) >= 5


@pytest.mark.asyncio
async def test_country_deep_dive_gather_and_build_empty_db(tmp_db):
    """CountryDeepDiveBriefing gather_data + build_sections + build_charts run without error on empty DB."""
    from app.db import get_db, release_db
    b = CountryDeepDiveBriefing(country_iso3="USA")
    db = await get_db()
    try:
        data = await b.gather_data(db, country_iso3="USA")
        b.build_sections(data)
        b.build_charts(data)
    finally:
        await release_db(db)
    # sections and cards are populated
    assert isinstance(b.sections, list)
    assert len(b.sections) >= 1
    assert isinstance(b.cards, list)


@pytest.mark.asyncio
async def test_country_deep_dive_title_contains_iso3(tmp_db):
    """CountryDeepDiveBriefing builds a title string that includes the ISO3."""
    from datetime import datetime, timezone

    from app.db import get_db, release_db
    b = CountryDeepDiveBriefing(country_iso3="BGD")
    db = await get_db()
    try:
        data = await b.gather_data(db, country_iso3="BGD")
        b.build_sections(data)
        b.build_charts(data)
    finally:
        await release_db(db)
    now = datetime.now(timezone.utc).strftime("%B %d, %Y")
    title = b.title_template.format(
        date=now,
        country_name=data.get("country_name", "BGD"),
        country_iso3="BGD",
    )
    assert "BGD" in title


@pytest.mark.asyncio
async def test_country_deep_dive_sections_built(tmp_db):
    """CountryDeepDiveBriefing build_sections populates at least 7 sections on empty DB."""
    from app.db import get_db, release_db
    b = CountryDeepDiveBriefing(country_iso3="DEU")
    db = await get_db()
    try:
        data = await b.gather_data(db, country_iso3="DEU")
        b.build_sections(data)
        b.build_charts(data)
    finally:
        await release_db(db)
    # Should have Executive Summary + 6 layer sections = at least 7
    assert len(b.sections) >= 7


@pytest.mark.asyncio
async def test_country_deep_dive_signal_color_stable():
    """_signal_color returns green for STABLE."""
    b = CountryDeepDiveBriefing()
    assert b._signal_color("STABLE") == "#059669"


@pytest.mark.asyncio
async def test_country_deep_dive_signal_color_crisis():
    """_signal_color returns red for CRISIS."""
    b = CountryDeepDiveBriefing()
    assert b._signal_color("CRISIS") == "#e11d48"


@pytest.mark.asyncio
async def test_country_deep_dive_signal_color_unknown():
    """_signal_color returns muted grey for unknown signal."""
    b = CountryDeepDiveBriefing()
    color = b._signal_color("UNKNOWN")
    assert color == "#64748b"
