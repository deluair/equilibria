"""Weekly Trade Flash briefing."""

from __future__ import annotations

import logging

from app.briefings.base import _ACCENT, _AMBER, BriefingBase

logger = logging.getLogger(__name__)

# Thresholds for flagging notable shifts
_CONCENTRATION_WARN = 0.25   # HHI above this flags high concentration
_GROWTH_THRESHOLD = 15.0     # YoY % change above this is "notable"


class TradeFlashBriefing(BriefingBase):
    briefing_type = "trade_flash"
    title_template = "Trade Flash: {date}"
    cadence = "weekly"

    def __init__(self):
        super().__init__()
        self.methodology_note = (
            "Trade Flash synthesizes recent bilateral trade data, export concentration "
            "(Herfindahl-Hirschman Index), and revealed comparative advantage shifts. "
            "Notable shifts are defined as year-over-year changes exceeding 15% in bilateral "
            "flows or significant HHI movements. Data sourced from BACI (via trade.db), "
            "UN Comtrade, and FRED trade balance series."
        )
        self.data_sources = ["BACI", "UN Comtrade", "FRED", "WTO"]

    async def gather_data(self, db, **kwargs) -> dict:
        """Query trade-related series for notable shifts and concentration changes."""
        data: dict = {
            "trade_balance": [],
            "top_flows": [],
            "concentration": [],
            "rca_shifts": [],
        }

        # Trade balance history (FRED BOPGSTB)
        series = await db.fetch_one(
            "SELECT id FROM data_series WHERE source = 'FRED' AND series_id = 'BOPGSTB'"
        )
        if series:
            data["trade_balance"] = await db.fetch_all(
                "SELECT date, value FROM data_points WHERE series_id = ? "
                "ORDER BY date DESC LIMIT 24",
                (series["id"],),
            )
            data["trade_balance"] = list(reversed(data["trade_balance"]))

        # Top bilateral flows from analysis_results (gravity/trade analysis)
        data["top_flows"] = await db.fetch_all(
            "SELECT country_iso3, result, score, created_at FROM analysis_results "
            "WHERE analysis_type = 'bilateral_trade' ORDER BY created_at DESC LIMIT 20"
        )

        # Concentration analysis results (HHI)
        data["concentration"] = await db.fetch_all(
            "SELECT country_iso3, result, score, created_at FROM analysis_results "
            "WHERE analysis_type = 'export_concentration' ORDER BY created_at DESC LIMIT 10"
        )

        # RCA shift results
        data["rca_shifts"] = await db.fetch_all(
            "SELECT country_iso3, result, score, created_at FROM analysis_results "
            "WHERE analysis_type = 'rca_shift' ORDER BY created_at DESC LIMIT 10"
        )

        return data

    def build_sections(self, data: dict) -> None:
        self.sections = []

        tb = data.get("trade_balance", [])
        top_flows = data.get("top_flows", [])
        concentration = data.get("concentration", [])
        rca_shifts = data.get("rca_shifts", [])

        # Executive Summary
        tb_latest = tb[-1] if tb else None
        tb_prev = tb[-2] if len(tb) >= 2 else None
        if tb_latest and tb_prev:
            change = tb_latest["value"] - tb_prev["value"]
            direction = "widened" if change < 0 else "narrowed"
            self.sections.append({
                "heading": "Executive Summary",
                "body": (
                    f'<p>The trade balance {direction} to ${tb_latest["value"]:.1f}B '
                    f'(change: ${change:+.1f}B). This flash report highlights notable '
                    f"bilateral trade shifts, concentration changes, and emerging "
                    f"comparative advantage patterns.</p>"
                ),
            })
        else:
            self.sections.append({
                "heading": "Executive Summary",
                "body": (
                    "<p>This flash report covers recent bilateral trade shifts, "
                    "export concentration changes, and emerging trade patterns. "
                    "Data collection is ongoing; more detail will appear as series populate.</p>"
                ),
            })

        # Notable Trade Shifts
        if top_flows:
            rows = ""
            for f in top_flows[:10]:
                rows += (
                    f'<tr><td style="padding: 6px 10px; border-bottom: 1px solid #e2e8f0;">'
                    f'{f["country_iso3"]}</td>'
                    f'<td style="padding: 6px 10px; border-bottom: 1px solid #e2e8f0;">'
                    f'{f["score"]:.1f}</td></tr>'
                )
            self.sections.append({
                "heading": "Notable Trade Shifts",
                "body": (
                    '<table style="width: 100%; border-collapse: collapse; font-size: 14px;">'
                    '<thead><tr>'
                    '<th style="text-align: left; padding: 8px 10px; border-bottom: 2px solid #e2e8f0;">'
                    'Partner</th>'
                    '<th style="text-align: left; padding: 8px 10px; border-bottom: 2px solid #e2e8f0;">'
                    'Score</th>'
                    '</tr></thead><tbody>'
                    f'{rows}</tbody></table>'
                ),
            })
        else:
            self.sections.append({
                "heading": "Notable Trade Shifts",
                "body": "<p>No bilateral trade analysis results available yet.</p>",
            })

        # Export Concentration
        high_conc = [c for c in concentration if c["score"] and c["score"] > _CONCENTRATION_WARN]
        if high_conc:
            items = "".join(
                f'<li>{c["country_iso3"]}: HHI = {c["score"]:.3f}</li>'
                for c in high_conc
            )
            self.sections.append({
                "heading": "Export Concentration Alerts",
                "body": (
                    f"<p>The following countries show elevated export concentration "
                    f"(HHI above {_CONCENTRATION_WARN}):</p>"
                    f"<ul style='margin: 0; padding-left: 20px;'>{items}</ul>"
                ),
            })
        else:
            self.sections.append({
                "heading": "Export Concentration",
                "body": (
                    "<p>No countries currently flagged for elevated export concentration.</p>"
                ),
            })

        # Emerging Patterns
        if rca_shifts:
            items = "".join(
                f'<li>{r["country_iso3"]}: RCA score {r["score"]:.2f}</li>'
                for r in rca_shifts[:5]
            )
            self.sections.append({
                "heading": "Emerging Comparative Advantage",
                "body": (
                    "<p>Recent shifts in revealed comparative advantage:</p>"
                    f"<ul style='margin: 0; padding-left: 20px;'>{items}</ul>"
                ),
            })
        else:
            self.sections.append({
                "heading": "Emerging Patterns",
                "body": (
                    "<p>RCA analysis pending. New comparative advantage signals "
                    "will be highlighted as data accumulates.</p>"
                ),
            })

        # Cards
        self.cards = []
        if tb_latest:
            color = "#059669" if tb_latest["value"] > 0 else "#e11d48"
            self.cards.append({
                "label": "Trade Balance",
                "value": f'${tb_latest["value"]:.1f}B',
                "color": color,
                "subtitle": f'As of {tb_latest["date"]}',
            })
        self.cards.append({
            "label": "Bilateral Flows Tracked",
            "value": str(len(top_flows)),
            "color": _ACCENT,
            "subtitle": "Recent analysis results",
        })
        self.cards.append({
            "label": "Concentration Alerts",
            "value": str(len(high_conc) if concentration else 0),
            "color": _AMBER if high_conc else "#059669",
            "subtitle": f"HHI > {_CONCENTRATION_WARN}",
        })
        self.cards.append({
            "label": "RCA Shifts",
            "value": str(len(rca_shifts)),
            "color": _ACCENT,
            "subtitle": "Countries with new patterns",
        })

    def build_charts(self, data: dict) -> None:
        self.charts = []

        # Trade balance trend
        tb = data.get("trade_balance", [])
        if tb:
            dates = [r["date"] for r in tb]
            values = [r["value"] for r in tb]
            chart_id = "trade_balance_trend"
            # Color bars by sign
            colors = ["#059669" if v >= 0 else "#e11d48" for v in values]
            self.charts.append(
                f'<div id="{chart_id}" style="width:100%;height:320px;"></div>'
                f'<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>'
                f'<script>'
                f'Plotly.newPlot("{chart_id}", [{{x: {dates}, y: {values}, '
                f'type: "bar", marker: {{color: {colors}}}}}], '
                f'{{title: "Trade Balance (Billions USD)", '
                f'margin: {{t: 40, r: 20, b: 40, l: 50}}, '
                f'paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", '
                f'xaxis: {{gridcolor: "#e2e8f0"}}, yaxis: {{gridcolor: "#e2e8f0"}}, '
                f'font: {{family: "-apple-system, sans-serif", color: "#0f172a"}}}}, '
                f'{{responsive: true}})'
                f'</script>'
            )
