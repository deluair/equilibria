"""Monthly Agricultural Outlook briefing."""

from __future__ import annotations

import logging

from app.briefings.base import _ACCENT, _AMBER, BriefingBase

logger = logging.getLogger(__name__)

_SERIES_MAP = {
    "food_price_index": ("WDI", "FP.CPI.TOTL.ZG"),         # Food CPI inflation
    "ag_production": ("WDI", "AG.PRD.FOOD.XD"),             # Food production index
    "ag_exports": ("WDI", "TX.VAL.AGRI.ZS.UN"),             # Ag exports (% of total exports)
    "ag_imports": ("WDI", "TM.VAL.AGRI.ZS.UN"),             # Ag imports (% of total imports)
    "food_security": ("WDI", "SN.ITK.DEFC.ZS"),             # Prevalence of undernourishment (%)
}


class AgriculturalOutlookBriefing(BriefingBase):
    briefing_type = "agricultural_outlook"
    title_template = "Agricultural Outlook: {date}"
    cadence = "monthly"

    def __init__(self):
        super().__init__()
        self.methodology_note = (
            "Monthly agricultural assessment drawing on WDI, FAO, and USDA series. "
            "Food price inflation is the annual percentage change in the consumer food "
            "price index. The food production index covers food crops and excludes "
            "non-food components. Agricultural trade shares use current USD. "
            "Food security is measured by FAO's prevalence of undernourishment, "
            "a three-year moving average. Trade balance is computed from export and "
            "import shares applied to total merchandise trade."
        )
        self.data_sources = ["World Bank WDI", "FAO", "USDA ERS", "UN Comtrade"]

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

        food_price = data.get("food_price_index", {}).get("latest")
        ag_prod = data.get("ag_production", {}).get("latest")
        ag_exports = data.get("ag_exports", {}).get("latest")
        ag_imports = data.get("ag_imports", {}).get("latest")
        food_sec = data.get("food_security", {}).get("latest")

        food_price_val = f'{food_price["value"]:.1f}%' if food_price else "N/A"
        ag_prod_val = f'{ag_prod["value"]:.1f}' if ag_prod else "N/A"
        ag_exports_val = f'{ag_exports["value"]:.1f}%' if ag_exports else "N/A"
        ag_imports_val = f'{ag_imports["value"]:.1f}%' if ag_imports else "N/A"
        food_sec_val = f'{food_sec["value"]:.1f}%' if food_sec else "N/A"

        # Price Trends
        price_note = ""
        if food_price:
            fp = food_price["value"]
            if fp > 10:
                price_note = (
                    f"Food price inflation is running at {food_price_val}, well above "
                    f"historical norms. Elevated food costs erode real household incomes, "
                    f"particularly among lower-income groups with high food expenditure shares."
                )
            elif fp > 5:
                price_note = (
                    f"Food price inflation of {food_price_val} is above the general "
                    f"consumer price target, warranting close monitoring for second-round "
                    f"effects on wages and core inflation."
                )
            else:
                price_note = (
                    f"Food price inflation of {food_price_val} is within a manageable range, "
                    f"posing limited stress on household food budgets at current levels."
                )
        else:
            price_note = "Food price inflation data unavailable."

        self.sections.append({
            "heading": "Price Trends",
            "body": f"<p>{price_note}</p>",
        })

        # Production
        self.sections.append({
            "heading": "Production",
            "body": (
                f"<p>The food production index stands at {ag_prod_val} (base period 2014-2016 = 100). "
                f"Production trends capture aggregate output dynamics across food crops. "
                f"Deviations from trend may reflect weather shocks, input price changes, "
                f"or structural shifts in cultivated area and yields.</p>"
            ),
        })

        # Trade Flows
        trade_note = ""
        if ag_exports and ag_imports:
            net = ag_exports["value"] - ag_imports["value"]
            direction = "net agricultural exporter" if net > 0 else "net agricultural importer"
            trade_note = (
                f"Agricultural exports account for {ag_exports_val} of total merchandise "
                f"exports, while agricultural imports represent {ag_imports_val} of total "
                f"imports. The economy is a {direction} by share of total trade."
            )
        elif ag_exports:
            trade_note = f"Agricultural exports represent {ag_exports_val} of total merchandise exports."
        else:
            trade_note = "Agricultural trade share data unavailable."

        self.sections.append({
            "heading": "Trade Flows",
            "body": f"<p>{trade_note}</p>",
        })

        # Food Security
        food_sec_note = ""
        if food_sec:
            fs = food_sec["value"]
            if fs < 2.5:
                food_sec_note = (
                    f"Undernourishment prevalence of {food_sec_val} is at or below the "
                    f"5% threshold used by FAO to classify populations as food-secure."
                )
            elif fs < 15:
                food_sec_note = (
                    f"Undernourishment at {food_sec_val} indicates moderate food insecurity. "
                    f"Targeted interventions in nutrition and social protection are warranted."
                )
            else:
                food_sec_note = (
                    f"Undernourishment at {food_sec_val} signals severe food insecurity. "
                    f"Structural interventions in agricultural productivity, access, and "
                    f"social safety nets are critical."
                )
        else:
            food_sec_note = "Food security indicator data unavailable."

        self.sections.append({
            "heading": "Food Security",
            "body": f"<p>{food_sec_note}</p>",
        })

        # Cards
        self.cards = []
        if food_price:
            color = "#059669" if food_price["value"] < 5 else (_AMBER if food_price["value"] < 10 else "#e11d48")
            self.cards.append({
                "label": "Food Price Inflation",
                "value": food_price_val,
                "color": color,
                "subtitle": f'Annual CPI food, {food_price["date"]}',
            })
        if ag_prod:
            color = "#059669" if ag_prod["value"] >= 100 else _AMBER
            self.cards.append({
                "label": "Production Index",
                "value": ag_prod_val,
                "color": color,
                "subtitle": f'FAO food index (2014-16=100), {ag_prod["date"]}',
            })
        if ag_exports and ag_imports:
            net = ag_exports["value"] - ag_imports["value"]
            net_str = f"{net:+.1f}pp"
            color = "#059669" if net > 0 else "#e11d48"
            self.cards.append({
                "label": "Trade Balance",
                "value": net_str,
                "color": color,
                "subtitle": "Exports minus imports (% of total trade)",
            })
        if food_sec:
            color = "#059669" if food_sec["value"] < 5 else (_AMBER if food_sec["value"] < 15 else "#e11d48")
            self.cards.append({
                "label": "Food Security Score",
                "value": food_sec_val,
                "color": color,
                "subtitle": f'Undernourishment %, {food_sec["date"]}',
            })

    def build_charts(self, data: dict) -> None:
        self.charts = []

        price_history = data.get("food_price_index", {}).get("history", [])
        prod_history = data.get("ag_production", {}).get("history", [])

        # Food price inflation trend
        if price_history:
            dates = [r["date"] for r in price_history]
            values = [r["value"] for r in price_history]
            chart_id = "food_price_trend"
            self.charts.append(
                f'<div id="{chart_id}" style="width:100%;height:300px;"></div>'
                f'<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>'
                f'<script>'
                f'Plotly.newPlot("{chart_id}", [{{x: {dates}, y: {values}, '
                f'type: "scatter", mode: "lines+markers", name: "Food Price Inflation (%)", '
                f'line: {{color: "#16a34a", width: 2}}, marker: {{size: 5, color: "#16a34a"}}}}], '
                f'{{title: "Food Price Inflation (%)", '
                f'margin: {{t: 40, r: 20, b: 40, l: 60}}, '
                f'paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", '
                f'xaxis: {{gridcolor: "#e2e8f0"}}, yaxis: {{gridcolor: "#e2e8f0"}}, '
                f'font: {{family: "-apple-system, sans-serif", color: "#0f172a"}}}}, '
                f'{{responsive: true}})'
                f'</script>'
            )

        # Production index trend
        if prod_history:
            dates_p = [r["date"] for r in prod_history]
            values_p = [r["value"] for r in prod_history]
            chart_id = "ag_prod_trend"
            self.charts.append(
                f'<div id="{chart_id}" style="width:100%;height:300px;"></div>'
                f'<script>'
                f'Plotly.newPlot("{chart_id}", [{{x: {dates_p}, y: {values_p}, '
                f'type: "scatter", mode: "lines+markers", name: "Food Production Index", '
                f'line: {{color: "{_ACCENT}", width: 2}}, marker: {{size: 5, color: "{_ACCENT}"}}}}], '
                f'{{title: "Food Production Index (2014-2016 = 100)", '
                f'margin: {{t: 40, r: 20, b: 40, l: 60}}, '
                f'paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", '
                f'xaxis: {{gridcolor: "#e2e8f0"}}, yaxis: {{gridcolor: "#e2e8f0"}}, '
                f'font: {{family: "-apple-system, sans-serif", color: "#0f172a"}}}}, '
                f'{{responsive: true}})'
                f'</script>'
            )
