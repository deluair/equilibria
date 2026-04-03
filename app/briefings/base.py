"""Base class for all Equilibria briefings."""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

BADGE_COLORS = {
    "economic_conditions": "#0891b2",
    "trade_flash": "#059669",
    "labor_pulse": "#d97706",
    "development_tracker": "#8b5cf6",
    "agricultural_outlook": "#16a34a",
    "policy_alert": "#e11d48",
    "country_deep_dive": "#0284c7",
}

# Design tokens
_BG = "#f8fafc"
_TEXT = "#0f172a"
_MUTED = "#64748b"
_ACCENT = "#0891b2"
_AMBER = "#d97706"
_BORDER = "#e2e8f0"
_CARD_BG = "#ffffff"


class BriefingBase(ABC):
    """Abstract base for briefing generators.

    Subclasses must set class attributes:
        briefing_type: str
        title_template: str
        cadence: str  (e.g. "weekly", "monthly", "on_demand")

    And implement:
        gather_data(db) -> dict
        build_sections(data)
        build_charts(data)
    """

    briefing_type: str = ""
    title_template: str = ""
    cadence: str = ""

    def __init__(self):
        self.sections: list[dict] = []       # {heading: str, body: str (HTML)}
        self.charts: list[str] = []           # Plotly HTML snippets
        self.cards: list[dict] = []           # {label, value, color, subtitle}
        self.methodology_note: str = ""
        self.data_sources: list[str] = []

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def gather_data(self, db) -> dict:
        """Query the database and return raw data dict."""

    @abstractmethod
    def build_sections(self, data: dict) -> None:
        """Populate self.sections from gathered data."""

    @abstractmethod
    def build_charts(self, data: dict) -> None:
        """Populate self.charts from gathered data."""

    # ------------------------------------------------------------------
    # HTML assembly
    # ------------------------------------------------------------------

    def assemble_html(self) -> str:
        """Build complete HTML briefing with inline styles. Professional publication quality."""

        badge_color = BADGE_COLORS.get(self.briefing_type, _ACCENT)
        now = datetime.now(timezone.utc).strftime("%B %d, %Y")
        title = self.title_template.format(date=now)

        parts: list[str] = []

        # Document wrapper
        parts.append(
            f'<div style="font-family: -apple-system, BlinkMacSystemFont, \'Segoe UI\', '
            f'Roboto, sans-serif; background: {_BG}; color: {_TEXT}; max-width: 900px; '
            f'margin: 0 auto; padding: 40px 32px; line-height: 1.65;">'
        )

        # Header
        parts.append(
            f'<div style="margin-bottom: 32px; border-bottom: 2px solid {badge_color}; '
            f'padding-bottom: 20px;">'
            f'<span style="display: inline-block; background: {badge_color}; color: #fff; '
            f'font-size: 11px; font-weight: 600; letter-spacing: 0.08em; '
            f'text-transform: uppercase; padding: 3px 10px; border-radius: 3px; '
            f'margin-bottom: 10px;">{self.briefing_type.replace("_", " ")}</span>'
            f'<h1 style="margin: 8px 0 4px; font-size: 26px; font-weight: 700; '
            f'color: {_TEXT};">{title}</h1>'
            f'<p style="margin: 0; font-size: 13px; color: {_MUTED};">Generated {now} '
            f'&middot; Equilibria Applied Economics Platform</p>'
            f'</div>'
        )

        # Cards grid
        if self.cards:
            parts.append(
                '<div style="display: grid; grid-template-columns: repeat(auto-fit, '
                'minmax(180px, 1fr)); gap: 16px; margin-bottom: 32px;">'
            )
            for card in self.cards:
                c = card.get("color", _ACCENT)
                parts.append(
                    f'<div style="background: {_CARD_BG}; border: 1px solid {_BORDER}; '
                    f'border-top: 3px solid {c}; border-radius: 6px; padding: 18px 16px;">'
                    f'<div style="font-size: 12px; color: {_MUTED}; font-weight: 500; '
                    f'text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 6px;">'
                    f'{card["label"]}</div>'
                    f'<div style="font-size: 28px; font-weight: 700; color: {c}; '
                    f'margin-bottom: 4px;">{card["value"]}</div>'
                    f'<div style="font-size: 12px; color: {_MUTED};">'
                    f'{card.get("subtitle", "")}</div>'
                    f'</div>'
                )
            parts.append('</div>')

        # Sections
        for section in self.sections:
            parts.append(
                f'<div style="margin-bottom: 28px;">'
                f'<h2 style="font-size: 18px; font-weight: 600; color: {_TEXT}; '
                f'margin: 0 0 12px; padding-bottom: 6px; '
                f'border-bottom: 1px solid {_BORDER};">{section["heading"]}</h2>'
                f'<div style="font-size: 15px; color: {_TEXT}; line-height: 1.7;">'
                f'{section["body"]}</div>'
                f'</div>'
            )

        # Charts
        for chart_html in self.charts:
            parts.append(
                f'<div style="margin-bottom: 28px; background: {_CARD_BG}; '
                f'border: 1px solid {_BORDER}; border-radius: 6px; padding: 16px; '
                f'overflow: hidden;">{chart_html}</div>'
            )

        # Methodology (collapsible)
        if self.methodology_note:
            parts.append(
                f'<details style="margin-bottom: 24px; border: 1px solid {_BORDER}; '
                f'border-radius: 6px; overflow: hidden;">'
                f'<summary style="padding: 12px 16px; font-size: 13px; font-weight: 600; '
                f'color: {_MUTED}; cursor: pointer; background: {_CARD_BG};">'
                f'Methodology</summary>'
                f'<div style="padding: 12px 16px; font-size: 13px; color: {_MUTED}; '
                f'line-height: 1.6; background: {_CARD_BG};">{self.methodology_note}</div>'
                f'</details>'
            )

        # Data sources footer
        if self.data_sources:
            sources_str = " &middot; ".join(self.data_sources)
            parts.append(
                f'<div style="margin-top: 32px; padding-top: 16px; '
                f'border-top: 1px solid {_BORDER}; font-size: 12px; color: {_MUTED};">'
                f'<strong>Data sources:</strong> {sources_str}</div>'
            )

        parts.append('</div>')
        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Generate and save
    # ------------------------------------------------------------------

    async def generate(self, db, **kwargs) -> dict:
        """Run the full pipeline: gather -> build -> assemble.

        Returns dict with title, briefing_type, body_html, methodology_note.
        """
        data = await self.gather_data(db, **kwargs)
        self.build_sections(data)
        self.build_charts(data)
        now = datetime.now(timezone.utc).strftime("%B %d, %Y")
        title = self.title_template.format(date=now, **kwargs)
        body_html = self.assemble_html()
        return {
            "title": title,
            "briefing_type": self.briefing_type,
            "body_html": body_html,
            "methodology_note": self.methodology_note,
        }

    async def save(self, result: dict, db, country_iso3: str = "GLOBAL") -> int:
        """Insert briefing into the briefings table. Returns the row id."""
        metadata = json.dumps({
            "briefing_type": result["briefing_type"],
            "methodology_note": result.get("methodology_note", ""),
            "data_sources": self.data_sources,
        })
        cursor = await db.conn.execute(
            "INSERT INTO briefings (country_iso3, title, content, layer_scores) "
            "VALUES (?, ?, ?, ?)",
            (country_iso3, result["title"], result["body_html"], metadata),
        )
        await db.conn.commit()
        row_id = cursor.lastrowid
        logger.info("Saved briefing id=%d type=%s", row_id, result["briefing_type"])
        return row_id
