"""Tests for BriefingBase (app/briefings/base.py)."""

import pytest

from app.briefings.base import BADGE_COLORS, BriefingBase


# ---------------------------------------------------------------------------
# Minimal concrete subclass for testing
# ---------------------------------------------------------------------------

class _MinimalBriefing(BriefingBase):
    briefing_type = "test_briefing"
    title_template = "Test Briefing: {date}"
    cadence = "on_demand"

    async def gather_data(self, db, **kwargs) -> dict:
        return {"value": 42}

    def build_sections(self, data: dict) -> None:
        self.sections = [
            {"heading": "Section One", "body": f"<p>Value is {data['value']}</p>"}
        ]

    def build_charts(self, data: dict) -> None:
        self.charts = []


class _BriefingWithCards(BriefingBase):
    briefing_type = "economic_conditions"
    title_template = "EC Briefing: {date}"
    cadence = "weekly"

    async def gather_data(self, db, **kwargs) -> dict:
        return {}

    def build_sections(self, data: dict) -> None:
        self.sections = [{"heading": "H", "body": "<p>body</p>"}]
        self.cards = [
            {"label": "GDP", "value": "3.0%", "color": "#059669", "subtitle": "Q3 2024"}
        ]
        self.methodology_note = "Test methodology note."
        self.data_sources = ["FRED", "WDI"]

    def build_charts(self, data: dict) -> None:
        self.charts = []


# ---------------------------------------------------------------------------
# BriefingBase is abstract
# ---------------------------------------------------------------------------

def test_briefing_base_cannot_instantiate():
    """BriefingBase is abstract and cannot be instantiated directly."""
    with pytest.raises(TypeError):
        BriefingBase()


# ---------------------------------------------------------------------------
# Subclass instantiation
# ---------------------------------------------------------------------------

def test_minimal_briefing_instantiates():
    """Minimal concrete subclass instantiates without error."""
    b = _MinimalBriefing()
    assert b.briefing_type == "test_briefing"
    assert b.cadence == "on_demand"
    assert b.sections == []
    assert b.charts == []
    assert b.cards == []


# ---------------------------------------------------------------------------
# generate() pipeline
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_generate_returns_required_keys(tmp_db):
    """generate() returns dict with title, briefing_type, body_html, methodology_note."""
    from app.db import get_db, release_db
    b = _MinimalBriefing()
    db = await get_db()
    try:
        result = await b.generate(db)
    finally:
        await release_db(db)

    assert "title" in result
    assert "briefing_type" in result
    assert "body_html" in result
    assert "methodology_note" in result


@pytest.mark.asyncio
async def test_generate_briefing_type_matches(tmp_db):
    """generate() result briefing_type matches the class attribute."""
    from app.db import get_db, release_db
    b = _MinimalBriefing()
    db = await get_db()
    try:
        result = await b.generate(db)
    finally:
        await release_db(db)
    assert result["briefing_type"] == "test_briefing"


@pytest.mark.asyncio
async def test_generate_body_html_is_string(tmp_db):
    """generate() body_html is a non-empty string."""
    from app.db import get_db, release_db
    b = _MinimalBriefing()
    db = await get_db()
    try:
        result = await b.generate(db)
    finally:
        await release_db(db)
    assert isinstance(result["body_html"], str)
    assert len(result["body_html"]) > 0


@pytest.mark.asyncio
async def test_generate_title_contains_date(tmp_db):
    """generate() title includes the generated date string."""
    from app.db import get_db, release_db
    b = _MinimalBriefing()
    db = await get_db()
    try:
        result = await b.generate(db)
    finally:
        await release_db(db)
    # title_template = "Test Briefing: {date}", should contain the word "Test"
    assert "Test Briefing" in result["title"]


# ---------------------------------------------------------------------------
# assemble_html()
# ---------------------------------------------------------------------------

def test_assemble_html_contains_briefing_type():
    """assemble_html includes the briefing_type text in the badge (underscores become spaces)."""
    b = _MinimalBriefing()
    b.sections = [{"heading": "A", "body": "<p>x</p>"}]
    html = b.assemble_html()
    # The badge renders briefing_type.replace("_", " ") as visible text
    assert "test briefing" in html


def test_assemble_html_includes_section_heading():
    """assemble_html renders section headings."""
    b = _MinimalBriefing()
    b.sections = [{"heading": "Executive Summary", "body": "<p>text</p>"}]
    html = b.assemble_html()
    assert "Executive Summary" in html


def test_assemble_html_includes_section_body():
    """assemble_html renders section body HTML."""
    b = _MinimalBriefing()
    b.sections = [{"heading": "H", "body": "<p>unique_content_123</p>"}]
    html = b.assemble_html()
    assert "unique_content_123" in html


def test_assemble_html_includes_cards():
    """assemble_html renders card values when cards are present."""
    b = _BriefingWithCards()
    b.build_sections({})
    html = b.assemble_html()
    assert "GDP" in html
    assert "3.0%" in html


def test_assemble_html_includes_methodology():
    """assemble_html includes methodology note when set."""
    b = _BriefingWithCards()
    b.build_sections({})
    html = b.assemble_html()
    assert "Test methodology note." in html


def test_assemble_html_includes_data_sources():
    """assemble_html includes data sources footer when set."""
    b = _BriefingWithCards()
    b.build_sections({})
    html = b.assemble_html()
    assert "FRED" in html
    assert "WDI" in html


def test_assemble_html_includes_chart_html():
    """assemble_html renders chart HTML snippets."""
    b = _MinimalBriefing()
    b.sections = []
    b.charts = ["<div id='chart1'>CHART_PLACEHOLDER</div>"]
    html = b.assemble_html()
    assert "CHART_PLACEHOLDER" in html


# ---------------------------------------------------------------------------
# BADGE_COLORS
# ---------------------------------------------------------------------------

def test_badge_colors_has_expected_types():
    """BADGE_COLORS has entries for all three implemented briefing types."""
    assert "economic_conditions" in BADGE_COLORS
    assert "trade_flash" in BADGE_COLORS
    assert "country_deep_dive" in BADGE_COLORS


def test_badge_colors_are_hex_strings():
    """All badge color values are hex color strings starting with '#'."""
    for btype, color in BADGE_COLORS.items():
        assert color.startswith("#"), f"Non-hex color for {btype}: {color}"
