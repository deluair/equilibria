"""Quarterly Development Tracker briefing."""

from __future__ import annotations

import logging

from app.briefings.base import _ACCENT, _AMBER, BriefingBase

logger = logging.getLogger(__name__)

_SERIES_MAP = {
    "gdp_per_capita": ("WDI", "NY.GDP.PCAP.KD.ZG"),    # GDP per capita growth (constant $)
    "hdi": ("WDI", "HD.HCI.OVRL"),                      # Human Capital Index (proxy for HDI)
    "gini": ("WDI", "SI.POV.GINI"),                     # Gini index
    "poverty_rate": ("WDI", "SI.POV.DDAY"),             # Poverty headcount ratio $2.15/day
}


class DevelopmentTrackerBriefing(BriefingBase):
    briefing_type = "development_tracker"
    title_template = "Development Tracker: {date}"
    cadence = "quarterly"

    def __init__(self):
        super().__init__()
        self.methodology_note = (
            "Quarterly development assessment using World Bank WDI series. GDP per capita "
            "growth is in constant local currency units. The Human Capital Index measures "
            "expected productivity of the next generation relative to a benchmark of "
            "complete education and full health. Gini index (0=perfect equality, "
            "100=perfect inequality). Poverty headcount at $2.15/day in 2017 PPP."
        )
        self.data_sources = ["World Bank WDI", "UNDP", "World Bank PovcalNet"]

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
                "ORDER BY date DESC LIMIT 20",
                (sid,),
            )
            data[key] = {
                "latest": latest,
                "history": list(reversed(history)),
            }

        return data

    def build_sections(self, data: dict) -> None:
        self.sections = []

        gdp_pc = data.get("gdp_per_capita", {}).get("latest")
        hdi = data.get("hdi", {}).get("latest")
        gini = data.get("gini", {}).get("latest")
        poverty = data.get("poverty_rate", {}).get("latest")

        gdp_val = f'{gdp_pc["value"]:.1f}%' if gdp_pc else "N/A"
        hdi_val = f'{hdi["value"]:.3f}' if hdi else "N/A"
        gini_val = f'{gini["value"]:.1f}' if gini else "N/A"
        poverty_val = f'{poverty["value"]:.1f}%' if poverty else "N/A"

        # Development Overview
        self.sections.append({
            "heading": "Development Overview",
            "body": (
                f"<p>GDP per capita growth is at {gdp_val}, the Human Capital Index stands "
                f"at {hdi_val}, the Gini coefficient is {gini_val}, and the extreme poverty "
                f"rate (at $2.15/day) is {poverty_val}. These four dimensions capture income "
                f"growth, human capital accumulation, distributional outcomes, and poverty "
                f"reduction progress.</p>"
            ),
        })

        # Convergence
        convergence_note = ""
        if gdp_pc:
            if gdp_pc["value"] > 4.0:
                convergence_note = (
                    f"GDP per capita growth of {gdp_val} is consistent with convergence "
                    f"dynamics toward upper-middle-income status, assuming sustained "
                    f"structural transformation."
                )
            elif gdp_pc["value"] > 0:
                convergence_note = (
                    f"GDP per capita growth of {gdp_val} represents positive but modest "
                    f"convergence. Faster gains require improved productivity and investment."
                )
            else:
                convergence_note = (
                    f"Negative per-capita growth of {gdp_val} points to divergence risk, "
                    f"with living standards potentially falling relative to frontier economies."
                )
        else:
            convergence_note = "GDP per capita data unavailable for convergence assessment."

        self.sections.append({
            "heading": "Convergence",
            "body": f"<p>{convergence_note}</p>",
        })

        # Inequality
        inequality_note = ""
        if gini:
            g = gini["value"]
            if g < 30:
                inequality_note = f"Gini of {gini_val} reflects relatively low inequality by global standards."
            elif g < 40:
                inequality_note = f"Gini of {gini_val} is in the moderate range, typical of middle-income economies."
            else:
                inequality_note = (
                    f"Gini of {gini_val} signals elevated inequality. High Gini values can "
                    f"constrain poverty reduction even at positive growth rates."
                )
        else:
            inequality_note = "Gini data unavailable; inequality assessment deferred."

        self.sections.append({
            "heading": "Inequality",
            "body": f"<p>{inequality_note} Distributional outcomes shape the inclusiveness of growth and social stability.</p>",
        })

        # Institutional Quality
        self.sections.append({
            "heading": "Institutional Quality",
            "body": (
                "<p>Institutional quality is a foundational driver of long-run development. "
                "Property rights, rule of law, control of corruption, and government "
                "effectiveness together determine the efficiency of resource allocation "
                "and the climate for investment. Governance metrics from V-Dem and the "
                "World Bank Governance Indicators are incorporated when available.</p>"
            ),
        })

        # Cards
        self.cards = []
        if gdp_pc:
            color = "#059669" if gdp_pc["value"] > 2.0 else (_AMBER if gdp_pc["value"] > 0 else "#e11d48")
            self.cards.append({
                "label": "GDP/cap Growth",
                "value": gdp_val,
                "color": color,
                "subtitle": f'Constant $, {gdp_pc["date"]}',
            })
        if hdi:
            color = "#059669" if hdi["value"] >= 0.7 else _AMBER
            self.cards.append({
                "label": "Human Capital Index",
                "value": hdi_val,
                "color": color,
                "subtitle": f'World Bank HCI, {hdi["date"]}',
            })
        if gini:
            color = "#059669" if gini["value"] < 35 else (_AMBER if gini["value"] < 45 else "#e11d48")
            self.cards.append({
                "label": "Gini",
                "value": gini_val,
                "color": color,
                "subtitle": f'0=equal, 100=unequal, {gini["date"]}',
            })
        if poverty:
            color = "#059669" if poverty["value"] < 3.0 else (_AMBER if poverty["value"] < 10.0 else "#e11d48")
            self.cards.append({
                "label": "Poverty Rate",
                "value": poverty_val,
                "color": color,
                "subtitle": f'$2.15/day 2017 PPP, {poverty["date"]}',
            })

    def build_charts(self, data: dict) -> None:
        self.charts = []

        gdp_history = data.get("gdp_per_capita", {}).get("history", [])
        gini_history = data.get("gini", {}).get("history", [])

        # GDP per capita growth trend
        if gdp_history:
            dates = [r["date"] for r in gdp_history]
            values = [r["value"] for r in gdp_history]
            chart_id = "gdp_pc_trend"
            self.charts.append(
                f'<div id="{chart_id}" style="width:100%;height:300px;"></div>'
                f'<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>'
                f'<script>'
                f'Plotly.newPlot("{chart_id}", [{{x: {dates}, y: {values}, '
                f'type: "scatter", mode: "lines+markers", name: "GDP/capita growth (%)", '
                f'line: {{color: "#8b5cf6", width: 2}}, marker: {{size: 5, color: "#8b5cf6"}}}}], '
                f'{{title: "GDP Per Capita Growth (%)", '
                f'margin: {{t: 40, r: 20, b: 40, l: 60}}, '
                f'paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", '
                f'xaxis: {{gridcolor: "#e2e8f0"}}, yaxis: {{gridcolor: "#e2e8f0"}}, '
                f'font: {{family: "-apple-system, sans-serif", color: "#0f172a"}}}}, '
                f'{{responsive: true}})'
                f'</script>'
            )

        # Gini trend
        if gini_history:
            dates_g = [r["date"] for r in gini_history]
            values_g = [r["value"] for r in gini_history]
            chart_id = "gini_trend"
            self.charts.append(
                f'<div id="{chart_id}" style="width:100%;height:300px;"></div>'
                f'<script>'
                f'Plotly.newPlot("{chart_id}", [{{x: {dates_g}, y: {values_g}, '
                f'type: "scatter", mode: "lines+markers", name: "Gini Index", '
                f'line: {{color: "{_AMBER}", width: 2}}, marker: {{size: 5, color: "{_AMBER}"}}}}], '
                f'{{title: "Gini Inequality Index", '
                f'margin: {{t: 40, r: 20, b: 40, l: 60}}, '
                f'paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", '
                f'xaxis: {{gridcolor: "#e2e8f0"}}, yaxis: {{gridcolor: "#e2e8f0", range: [0, 70]}}, '
                f'font: {{family: "-apple-system, sans-serif", color: "#0f172a"}}}}, '
                f'{{responsive: true}})'
                f'</script>'
            )
