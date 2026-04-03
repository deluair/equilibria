"""Event-driven Policy Alert briefing."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.briefings.base import _ACCENT, _AMBER, BriefingBase

logger = logging.getLogger(__name__)

# Threshold definitions: (series_source, series_id, direction, threshold, description)
# direction: "above" triggers alert when value > threshold; "below" when value < threshold
_ALERT_THRESHOLDS = [
    ("FRED", "UNRATE",       "above", 6.0,   "Unemployment Rate",        "%",   "above 6.0%"),
    ("FRED", "CPIAUCSL",     "above", 5.0,   "CPI Inflation",            "%",   "above 5.0%"),
    ("FRED", "NFCI",         "above", 0.5,   "Financial Conditions (NFCI)", "", "tighter than +0.5"),
    ("FRED", "A191RL1Q225SBEA", "below", 0.0, "Real GDP Growth",         "%",   "negative growth"),
    ("FRED", "BOPGSTB",      "below", -100.0, "Trade Balance",           "B$",  "deficit below -$100B"),
    ("WDI",  "FP.CPI.TOTL.ZG", "above", 10.0, "Food Price Inflation",   "%",   "above 10%"),
    ("WDI",  "SI.POV.DDAY",  "above", 15.0,  "Extreme Poverty Rate",    "%",   "above 15%"),
]


class PolicyAlertBriefing(BriefingBase):
    briefing_type = "policy_alert"
    title_template = "Policy Alert: {date}"
    cadence = "event"

    def __init__(self):
        super().__init__()
        self.methodology_note = (
            "Policy alerts are triggered when tracked indicators breach predefined "
            "thresholds. Thresholds are set at levels historically associated with "
            "heightened policy urgency: unemployment above 6%, CPI above 5%, NFCI "
            "above +0.5 (tight), GDP growth negative, trade deficit below -$100B, "
            "food price inflation above 10%, and extreme poverty above 15%. "
            "Alert severity is determined by the number and type of simultaneous breaches."
        )
        self.data_sources = ["FRED", "World Bank WDI", "BLS", "BEA"]

    async def gather_data(self, db, **kwargs) -> dict:
        data: dict = {
            "breaches": [],
            "checked": [],
            "last_alert_date": None,
        }

        for source, series_id, direction, threshold, label, unit, breach_desc in _ALERT_THRESHOLDS:
            series = await db.fetch_one(
                "SELECT id FROM data_series WHERE source = ? AND series_id = ?",
                (source, series_id),
            )
            if series is None:
                data["checked"].append({
                    "label": label,
                    "status": "no_data",
                    "value": None,
                    "threshold": threshold,
                    "direction": direction,
                    "breach_desc": breach_desc,
                    "unit": unit,
                })
                continue

            sid = series["id"]
            latest = await db.fetch_one(
                "SELECT date, value FROM data_points WHERE series_id = ? "
                "ORDER BY date DESC LIMIT 1",
                (sid,),
            )

            # History for context (last 12 observations)
            history = await db.fetch_all(
                "SELECT date, value FROM data_points WHERE series_id = ? "
                "ORDER BY date DESC LIMIT 12",
                (sid,),
            )

            breached = False
            if latest is not None:
                val = latest["value"]
                if direction == "above" and val > threshold:
                    breached = True
                elif direction == "below" and val < threshold:
                    breached = True

            entry = {
                "label": label,
                "source": source,
                "series_id": series_id,
                "direction": direction,
                "threshold": threshold,
                "breach_desc": breach_desc,
                "unit": unit,
                "latest": latest,
                "history": list(reversed(history)),
                "breached": breached,
                "status": "breach" if breached else "ok",
            }
            data["checked"].append(entry)
            if breached:
                data["breaches"].append(entry)

        # Check for the last alert briefing in the briefings table
        last_alert = await db.fetch_one(
            "SELECT created_at FROM briefings WHERE title LIKE 'Policy Alert:%' "
            "ORDER BY created_at DESC LIMIT 1",
        )
        if last_alert:
            data["last_alert_date"] = last_alert["created_at"]

        return data

    def build_sections(self, data: dict) -> None:
        self.sections = []

        breaches = data.get("breaches", [])
        checked = data.get("checked", [])
        last_alert_date = data.get("last_alert_date")
        n_breached = len(breaches)
        n_checked = len(checked)

        # Alert level
        if n_breached == 0:
            level = "NORMAL"
            level_color = "#059669"
            level_desc = "No threshold breaches detected across monitored indicators."
        elif n_breached <= 2:
            level = "WATCH"
            level_color = _AMBER
            level_desc = f"{n_breached} indicator(s) have breached alert thresholds. Elevated monitoring warranted."
        elif n_breached <= 4:
            level = "STRESS"
            level_color = "#ea580c"
            level_desc = f"{n_breached} indicators in breach. Coordinated policy response should be considered."
        else:
            level = "CRISIS"
            level_color = "#e11d48"
            level_desc = f"{n_breached} indicators simultaneously in breach. Immediate policy action required."

        # Alert Summary
        summary_body = f'<p><strong style="color:{level_color};">[{level}]</strong> {level_desc}</p>'
        if n_breached > 0:
            breach_list = "".join(
                f'<li><strong>{b["label"]}</strong>: '
                f'{b["latest"]["value"]:.2f}{b["unit"]} ({b["breach_desc"]})</li>'
                for b in breaches if b["latest"]
            )
            summary_body += f'<ul style="margin: 8px 0 0; padding-left: 20px;">{breach_list}</ul>'

        self.sections.append({
            "heading": "Alert Summary",
            "body": summary_body,
        })

        # Affected Indicators
        if n_breached > 0:
            rows = "".join(
                f'<tr>'
                f'<td style="padding: 6px 10px; border-bottom: 1px solid #e2e8f0;">{b["label"]}</td>'
                f'<td style="padding: 6px 10px; border-bottom: 1px solid #e2e8f0; color:#e11d48; font-weight:600;">'
                f'{b["latest"]["value"]:.2f}{b["unit"]}</td>'
                f'<td style="padding: 6px 10px; border-bottom: 1px solid #e2e8f0; color:#64748b;">'
                f'{b["breach_desc"]}</td>'
                f'<td style="padding: 6px 10px; border-bottom: 1px solid #e2e8f0; color:#64748b;">'
                f'{b["latest"]["date"]}</td>'
                f'</tr>'
                for b in breaches if b["latest"]
            )
            table = (
                '<table style="width:100%; border-collapse: collapse; font-size: 14px;">'
                '<thead><tr style="background:#f8fafc;">'
                '<th style="padding: 6px 10px; text-align:left; color:#64748b; font-weight:600;">Indicator</th>'
                '<th style="padding: 6px 10px; text-align:left; color:#64748b; font-weight:600;">Current Value</th>'
                '<th style="padding: 6px 10px; text-align:left; color:#64748b; font-weight:600;">Threshold Breach</th>'
                '<th style="padding: 6px 10px; text-align:left; color:#64748b; font-weight:600;">As Of</th>'
                '</tr></thead>'
                f'<tbody>{rows}</tbody></table>'
            )
            self.sections.append({
                "heading": "Affected Indicators",
                "body": table,
            })
        else:
            self.sections.append({
                "heading": "Affected Indicators",
                "body": f"<p>All {n_checked} monitored indicators are within normal thresholds.</p>",
            })

        # Historical Context
        context_body = (
            f"<p>{n_checked} indicators were evaluated against predefined policy thresholds. "
            f"{n_breached} breach(es) detected. "
        )
        if last_alert_date:
            context_body += f"The previous Policy Alert was recorded on {last_alert_date}."
        else:
            context_body += "No previous Policy Alert found in the briefings record."
        context_body += (
            " Historical breach patterns help distinguish cyclical stress from structural "
            "deterioration. Single-indicator breaches are typically cyclical; multi-indicator "
            "simultaneous breaches warrant deeper structural diagnosis.</p>"
        )
        self.sections.append({
            "heading": "Historical Context",
            "body": context_body,
        })

        # Recommended Action
        if n_breached == 0:
            action = (
                "No immediate policy action indicated. Continue routine monitoring. "
                "Review thresholds quarterly to ensure they reflect current analytical priors."
            )
        elif level == "WATCH":
            action = (
                "Increase monitoring frequency for breaching indicators. "
                "Prepare contingency analysis. Communicate watch status to relevant policy teams."
            )
        elif level == "STRESS":
            action = (
                "Initiate cross-agency policy review. Prepare response options across fiscal, "
                "monetary, and trade policy levers. Brief senior decision-makers."
            )
        else:
            action = (
                "Convene emergency policy review. Activate crisis response protocols. "
                "Coordinate with international partners if external shocks are implicated. "
                "Escalate to highest decision-making authority."
            )

        self.sections.append({
            "heading": "Recommended Action",
            "body": f"<p>{action}</p>",
        })

        # Cards
        self.cards = []
        self.cards.append({
            "label": "Alert Level",
            "value": level,
            "color": level_color,
            "subtitle": f"{n_checked} indicators monitored",
        })
        self.cards.append({
            "label": "Indicators Breached",
            "value": str(n_breached),
            "color": "#e11d48" if n_breached > 0 else "#059669",
            "subtitle": f"of {n_checked} thresholds checked",
        })
        if last_alert_date:
            self.cards.append({
                "label": "Last Alert",
                "value": str(last_alert_date)[:10],
                "color": _ACCENT,
                "subtitle": "Previous policy alert date",
            })
        else:
            self.cards.append({
                "label": "Last Alert",
                "value": "None",
                "color": _ACCENT,
                "subtitle": "No prior alerts on record",
            })

    def build_charts(self, data: dict) -> None:
        self.charts = []

        # Breach status bar chart
        checked = data.get("checked", [])
        if not checked:
            return

        labels = []
        colors = []
        values = []

        for entry in checked:
            if entry["latest"] is None:
                continue
            labels.append(entry["label"])
            val = entry["latest"]["value"]
            threshold = entry["threshold"]
            # Normalize: ratio of current value to threshold (1.0 = at threshold)
            if threshold != 0:
                ratio = val / threshold
            else:
                ratio = 1.0
            values.append(round(ratio, 3))
            colors.append("#e11d48" if entry["breached"] else "#059669")

        if not labels:
            return

        chart_id = "alert_status_bars"
        self.charts.append(
            f'<div id="{chart_id}" style="width:100%;height:360px;"></div>'
            f'<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>'
            f'<script>'
            f'Plotly.newPlot("{chart_id}", [{{x: {values}, y: {labels}, '
            f'type: "bar", orientation: "h", '
            f'marker: {{color: {colors}}}, '
            f'name: "Value / Threshold ratio"}}], '
            f'{{title: "Indicator Status (value / threshold; 1.0 = threshold)", '
            f'xaxis: {{title: "Ratio to threshold", gridcolor: "#e2e8f0"}}, '
            f'yaxis: {{automargin: true, gridcolor: "#e2e8f0"}}, '
            f'shapes: [{{type: "line", x0: 1, x1: 1, y0: -0.5, y1: {len(labels) - 0.5}, '
            f'line: {{color: "{_AMBER}", width: 2, dash: "dot"}}}}], '
            f'margin: {{t: 50, r: 30, b: 60, l: 170}}, '
            f'paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", '
            f'font: {{family: "-apple-system, sans-serif", color: "#0f172a"}}}}, '
            f'{{responsive: true}})'
            f'</script>'
        )
