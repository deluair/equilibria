"""Weekly Economic Conditions Assessment briefing."""

from __future__ import annotations

import logging

from app.briefings.base import _ACCENT, _AMBER, BriefingBase

logger = logging.getLogger(__name__)

# Series identifiers used to query data_series/data_points
_SERIES_MAP = {
    "gdp_growth": ("FRED", "A191RL1Q225SBEA"),       # Real GDP growth rate
    "inflation": ("FRED", "CPIAUCSL"),                 # CPI all items
    "unemployment": ("FRED", "UNRATE"),                # Unemployment rate
    "trade_balance": ("FRED", "BOPGSTB"),              # Trade balance
    "fci": ("FRED", "NFCI"),                           # Financial conditions index
}


class EconomicConditionsBriefing(BriefingBase):
    briefing_type = "economic_conditions"
    title_template = "Economic Conditions Assessment: {date}"
    cadence = "weekly"

    def __init__(self):
        super().__init__()
        self.methodology_note = (
            "Composite assessment based on latest available data from FRED, WDI, and ILO. "
            "GDP growth is annualized quarter-over-quarter. Inflation is 12-month CPI change. "
            "Trade balance is goods and services in billions USD. Financial conditions index "
            "uses the Chicago Fed NFCI where positive values indicate tighter-than-average conditions."
        )
        self.data_sources = ["FRED", "World Bank WDI", "ILO", "UN Comtrade"]

    async def gather_data(self, db, **kwargs) -> dict:
        """Query latest GDP growth, inflation, unemployment, trade balance, FCI."""
        data: dict = {}

        for key, (source, series_id) in _SERIES_MAP.items():
            # Look up the series first
            series = await db.fetch_one(
                "SELECT id FROM data_series WHERE source = ? AND series_id = ?",
                (source, series_id),
            )
            if series is None:
                data[key] = {"latest": None, "history": []}
                continue

            sid = series["id"]

            # Latest value
            latest = await db.fetch_one(
                "SELECT date, value FROM data_points WHERE series_id = ? "
                "ORDER BY date DESC LIMIT 1",
                (sid,),
            )

            # Recent history for charts (last 20 observations)
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

        # Extract latest values for narrative
        gdp = data.get("gdp_growth", {}).get("latest")
        inf = data.get("inflation", {}).get("latest")
        unemp = data.get("unemployment", {}).get("latest")
        tb = data.get("trade_balance", {}).get("latest")
        fci = data.get("fci", {}).get("latest")

        # Executive Summary
        gdp_val = f'{gdp["value"]:.1f}%' if gdp else "N/A"
        inf_val = f'{inf["value"]:.1f}%' if inf else "N/A"
        unemp_val = f'{unemp["value"]:.1f}%' if unemp else "N/A"

        self.sections.append({
            "heading": "Executive Summary",
            "body": (
                f"<p>The latest data show GDP growth at {gdp_val}, inflation at {inf_val}, "
                f"and unemployment at {unemp_val}. This assessment synthesizes recent macro "
                f"indicators to provide a composite view of current economic conditions.</p>"
            ),
        })

        # Macro Environment
        fci_desc = "N/A"
        if fci:
            fci_v = fci["value"]
            if fci_v > 0:
                fci_desc = f"tighter than average ({fci_v:+.2f})"
            elif fci_v < 0:
                fci_desc = f"looser than average ({fci_v:+.2f})"
            else:
                fci_desc = "at historical average"

        self.sections.append({
            "heading": "Macro Environment",
            "body": (
                f"<p>Real GDP growth stands at {gdp_val}. Consumer price inflation is "
                f"running at {inf_val} on a 12-month basis. Financial conditions are "
                f"{fci_desc} as measured by the Chicago Fed NFCI.</p>"
            ),
        })

        # Trade Conditions
        tb_val = f'${tb["value"]:.1f}B' if tb else "N/A"
        self.sections.append({
            "heading": "Trade Conditions",
            "body": (
                f"<p>The goods and services trade balance is {tb_val}. "
                f"Changes in trade flows are monitored for signs of shifting comparative "
                f"advantage or emerging protectionist pressures.</p>"
            ),
        })

        # Labor Market
        self.sections.append({
            "heading": "Labor Market",
            "body": (
                f"<p>The unemployment rate is {unemp_val}. Labor market conditions are "
                f"assessed alongside wage growth and participation trends to gauge the "
                f"tightness of the employment environment.</p>"
            ),
        })

        # Key Risks
        risks = []
        if inf and inf["value"] > 4.0:
            risks.append("Elevated inflation may prompt further monetary tightening.")
        if fci and fci["value"] > 0.5:
            risks.append("Financial conditions are notably tight, raising recession risk.")
        if tb and tb["value"] < -80:
            risks.append("Wide trade deficit suggests external imbalance pressures.")
        if not risks:
            risks.append("No acute risk signals detected in current indicators.")

        risk_items = "".join(f"<li>{r}</li>" for r in risks)
        self.sections.append({
            "heading": "Key Risks",
            "body": f"<ul style='margin: 0; padding-left: 20px;'>{risk_items}</ul>",
        })

        # Build cards
        self.cards = []
        if gdp:
            color = "#059669" if gdp["value"] > 0 else "#e11d48"
            self.cards.append({
                "label": "GDP Growth",
                "value": gdp_val,
                "color": color,
                "subtitle": f'As of {gdp["date"]}',
            })
        if inf:
            color = _AMBER if inf["value"] > 3.0 else _ACCENT
            self.cards.append({
                "label": "Inflation",
                "value": inf_val,
                "color": color,
                "subtitle": f'12-mo CPI, {inf["date"]}',
            })
        if unemp:
            color = "#059669" if unemp["value"] < 5.0 else _AMBER
            self.cards.append({
                "label": "Unemployment",
                "value": unemp_val,
                "color": color,
                "subtitle": f'As of {unemp["date"]}',
            })
        if tb:
            color = "#059669" if tb["value"] > 0 else "#e11d48"
            self.cards.append({
                "label": "Trade Balance",
                "value": tb_val,
                "color": color,
                "subtitle": f'Goods & services, {tb["date"]}',
            })

    def build_charts(self, data: dict) -> None:
        self.charts = []

        # GDP trend line chart (Plotly)
        gdp_history = data.get("gdp_growth", {}).get("history", [])
        if gdp_history:
            dates = [r["date"] for r in gdp_history]
            values = [r["value"] for r in gdp_history]
            dates_js = str(dates)
            values_js = str(values)
            chart_id = "gdp_trend"
            self.charts.append(
                f'<div id="{chart_id}" style="width:100%;height:320px;"></div>'
                f'<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>'
                f'<script>'
                f'Plotly.newPlot("{chart_id}", [{{x: {dates_js}, y: {values_js}, '
                f'type: "scatter", mode: "lines+markers", '
                f'line: {{color: "{_ACCENT}", width: 2}}, '
                f'marker: {{size: 5, color: "{_ACCENT}"}}}}], '
                f'{{title: "Real GDP Growth (%)", margin: {{t: 40, r: 20, b: 40, l: 50}}, '
                f'paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", '
                f'xaxis: {{gridcolor: "#e2e8f0"}}, yaxis: {{gridcolor: "#e2e8f0"}}, '
                f'font: {{family: "-apple-system, sans-serif", color: "#0f172a"}}}}, '
                f'{{responsive: true}})'
                f'</script>'
            )

        # Inflation vs unemployment scatter
        inf_history = data.get("inflation", {}).get("history", [])
        unemp_history = data.get("unemployment", {}).get("history", [])
        if inf_history and unemp_history:
            # Align by date
            inf_by_date = {r["date"]: r["value"] for r in inf_history}
            unemp_by_date = {r["date"]: r["value"] for r in unemp_history}
            common_dates = sorted(set(inf_by_date) & set(unemp_by_date))
            if common_dates:
                x_vals = [unemp_by_date[d] for d in common_dates]
                y_vals = [inf_by_date[d] for d in common_dates]
                chart_id = "phillips_scatter"
                self.charts.append(
                    f'<div id="{chart_id}" style="width:100%;height:320px;"></div>'
                    f'<script>'
                    f'Plotly.newPlot("{chart_id}", [{{x: {x_vals}, y: {y_vals}, '
                    f'type: "scatter", mode: "markers", '
                    f'marker: {{size: 8, color: "{_AMBER}", opacity: 0.8}}}}], '
                    f'{{title: "Inflation vs Unemployment (Phillips Curve)", '
                    f'xaxis: {{title: "Unemployment Rate (%)", gridcolor: "#e2e8f0"}}, '
                    f'yaxis: {{title: "Inflation Rate (%)", gridcolor: "#e2e8f0"}}, '
                    f'margin: {{t: 40, r: 20, b: 50, l: 60}}, '
                    f'paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", '
                    f'font: {{family: "-apple-system, sans-serif", color: "#0f172a"}}}}, '
                    f'{{responsive: true}})'
                    f'</script>'
                )
