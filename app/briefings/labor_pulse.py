"""Monthly Labor Pulse briefing."""

from __future__ import annotations

import logging

from app.briefings.base import _ACCENT, _AMBER, BriefingBase

logger = logging.getLogger(__name__)

_SERIES_MAP = {
    "unemployment": ("FRED", "UNRATE"),        # Unemployment rate
    "lfpr": ("FRED", "CIVPART"),               # Labor force participation rate
    "wages": ("FRED", "CES0500000003"),         # Average hourly earnings
    "job_openings": ("FRED", "JTSJOL"),        # JOLTS: job openings
    "hires": ("FRED", "JTSHIL"),               # JOLTS: hires
}


class LaborPulseBriefing(BriefingBase):
    briefing_type = "labor_pulse"
    title_template = "Labor Pulse: {date}"
    cadence = "monthly"

    def __init__(self):
        super().__init__()
        self.methodology_note = (
            "Monthly labor market assessment using FRED series. Unemployment rate is the "
            "U-3 measure (seasonally adjusted). LFPR covers the civilian non-institutional "
            "population 16+. Average hourly earnings are for all private employees. "
            "V/U ratio is job openings divided by unemployed persons, a gauge of labor "
            "market tightness. JOLTS data have a one-month publication lag."
        )
        self.data_sources = ["FRED", "BLS JOLTS", "BLS CES"]

    async def gather_data(self, db, **kwargs) -> dict:
        data: dict = {}

        for key, (source, series_id) in _SERIES_MAP.items():
            series = await db.fetch_one(
                "SELECT id FROM data_series WHERE source = ? AND series_id = ?",
                (source, series_id),
            )
            if series is None:
                data[key] = {"latest": None, "history": []}
                continue

            sid = series["id"]

            latest = await db.fetch_one(
                "SELECT date, value FROM data_points WHERE series_id = ? "
                "ORDER BY date DESC LIMIT 1",
                (sid,),
            )
            history = await db.fetch_all(
                "SELECT date, value FROM data_points WHERE series_id = ? "
                "ORDER BY date DESC LIMIT 24",
                (sid,),
            )
            data[key] = {
                "latest": latest,
                "history": list(reversed(history)),
            }

        # Compute V/U ratio from latest job openings and unemployed (FRED UNEMPLOY)
        vu_series = await db.fetch_one(
            "SELECT id FROM data_series WHERE source = 'FRED' AND series_id = 'UNEMPLOY'",
        )
        data["unemployed_level"] = {"latest": None}
        if vu_series:
            unemp_level = await db.fetch_one(
                "SELECT date, value FROM data_points WHERE series_id = ? "
                "ORDER BY date DESC LIMIT 1",
                (vu_series["id"],),
            )
            data["unemployed_level"] = {"latest": unemp_level}

        return data

    def build_sections(self, data: dict) -> None:
        self.sections = []

        unemp = data.get("unemployment", {}).get("latest")
        lfpr = data.get("lfpr", {}).get("latest")
        wages = data.get("wages", {}).get("latest")
        openings = data.get("job_openings", {}).get("latest")
        hires = data.get("hires", {}).get("latest")
        unemployed_level = data.get("unemployed_level", {}).get("latest")

        unemp_val = f'{unemp["value"]:.1f}%' if unemp else "N/A"
        lfpr_val = f'{lfpr["value"]:.1f}%' if lfpr else "N/A"
        wages_val = f'${wages["value"]:.2f}' if wages else "N/A"

        # V/U ratio
        vu_str = "N/A"
        vu_num = None
        if openings and unemployed_level and unemployed_level["value"]:
            # openings in thousands, unemployed_level in thousands
            vu_num = openings["value"] / unemployed_level["value"]
            vu_str = f"{vu_num:.2f}"

        # Labor Market Overview
        self.sections.append({
            "heading": "Labor Market Overview",
            "body": (
                f"<p>The unemployment rate stands at {unemp_val}, with the labor force "
                f"participation rate at {lfpr_val}. Average hourly earnings for private "
                f"sector employees are {wages_val}. The vacancy-to-unemployed ratio is "
                f"{vu_str}, reflecting the current degree of labor market tightness.</p>"
            ),
        })

        # Employment Trends
        hires_val = f'{hires["value"] / 1000:.2f}M' if hires else "N/A"
        self.sections.append({
            "heading": "Employment Trends",
            "body": (
                f"<p>Monthly hires are running at approximately {hires_val}. "
                f"Job openings and hiring flows from JOLTS provide leading signals "
                f"of future employment growth. Sustained hires above separations "
                f"indicate net job creation in the economy.</p>"
            ),
        })

        # Wage Dynamics
        wage_pressure = ""
        if wages and wages["value"] > 30:
            wage_pressure = (
                "Elevated wage levels may sustain services inflation, keeping "
                "pressure on the Federal Reserve."
            )
        else:
            wage_pressure = (
                "Wage growth remains within a range consistent with the inflation target "
                "assuming trend productivity."
            )
        self.sections.append({
            "heading": "Wage Dynamics",
            "body": (
                f"<p>Average hourly earnings are {wages_val}. {wage_pressure} "
                f"Real wage trends depend on the interaction of nominal wage growth "
                f"with prevailing consumer price inflation.</p>"
            ),
        })

        # Key Risks
        risks = []
        if unemp and unemp["value"] > 5.0:
            risks.append("Unemployment above 5% signals slack; risk of further softening if demand cools.")
        if vu_num is not None and vu_num < 1.0:
            risks.append("V/U ratio below 1.0: labor demand no longer exceeds unemployed workers.")
        if lfpr and lfpr["value"] < 62.0:
            risks.append("Low participation rate limits effective labor supply and potential output.")
        if not risks:
            risks.append("No acute labor market stress signals in current indicators.")

        risk_items = "".join(f"<li>{r}</li>" for r in risks)
        self.sections.append({
            "heading": "Key Risks",
            "body": f"<ul style='margin: 0; padding-left: 20px;'>{risk_items}</ul>",
        })

        # Cards
        self.cards = []
        if unemp:
            color = "#059669" if unemp["value"] < 5.0 else _AMBER
            self.cards.append({
                "label": "Unemployment Rate",
                "value": unemp_val,
                "color": color,
                "subtitle": f'U-3, {unemp["date"]}',
            })
        if lfpr:
            color = "#059669" if lfpr["value"] >= 62.5 else _AMBER
            self.cards.append({
                "label": "LFPR",
                "value": lfpr_val,
                "color": color,
                "subtitle": f'Civilian 16+, {lfpr["date"]}',
            })
        if wages:
            self.cards.append({
                "label": "Wage Growth",
                "value": wages_val,
                "color": _ACCENT,
                "subtitle": f'Avg hourly earnings, {wages["date"]}',
            })
        if vu_num is not None:
            color = "#059669" if vu_num >= 1.0 else _AMBER
            self.cards.append({
                "label": "V/U Ratio",
                "value": vu_str,
                "color": color,
                "subtitle": "Openings per unemployed worker",
            })

    def build_charts(self, data: dict) -> None:
        self.charts = []

        unemp_history = data.get("unemployment", {}).get("history", [])
        wages_history = data.get("wages", {}).get("history", [])

        if unemp_history or wages_history:
            traces = []
            if unemp_history:
                dates_u = [r["date"] for r in unemp_history]
                vals_u = [r["value"] for r in unemp_history]
                traces.append(
                    f'{{x: {dates_u}, y: {vals_u}, type: "scatter", mode: "lines", '
                    f'name: "Unemployment (%)", yaxis: "y1", '
                    f'line: {{color: "#e11d48", width: 2}}}}'
                )
            if wages_history:
                dates_w = [r["date"] for r in wages_history]
                vals_w = [r["value"] for r in wages_history]
                traces.append(
                    f'{{x: {dates_w}, y: {vals_w}, type: "scatter", mode: "lines", '
                    f'name: "Avg Hourly Earnings ($)", yaxis: "y2", '
                    f'line: {{color: "{_ACCENT}", width: 2}}}}'
                )

            chart_id = "labor_dual"
            layout = (
                f'{{title: "Unemployment Rate & Average Hourly Earnings", '
                f'yaxis: {{title: "Unemployment (%)", gridcolor: "#e2e8f0"}}, '
                f'yaxis2: {{title: "Avg Hourly Earnings ($)", overlaying: "y", side: "right", gridcolor: "#e2e8f0"}}, '
                f'legend: {{x: 0, y: 1.1, orientation: "h"}}, '
                f'margin: {{t: 50, r: 60, b: 40, l: 60}}, '
                f'paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", '
                f'font: {{family: "-apple-system, sans-serif", color: "#0f172a"}}}}'
            )
            self.charts.append(
                f'<div id="{chart_id}" style="width:100%;height:340px;"></div>'
                f'<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>'
                f'<script>'
                f'Plotly.newPlot("{chart_id}", [{", ".join(traces)}], {layout}, {{responsive: true}})'
                f'</script>'
            )
