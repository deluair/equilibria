"""On-demand Country Deep Dive briefing. Full 6-layer analysis for a specific country."""

from __future__ import annotations

import logging

from app.briefings.base import _ACCENT, _AMBER, BriefingBase

logger = logging.getLogger(__name__)

# Layer labels for the 6-layer framework
_LAYERS = [
    ("trade", "Trade"),
    ("macro", "Macro"),
    ("labor", "Labor"),
    ("development", "Development"),
    ("agricultural", "Agricultural"),
    ("integration", "Integration"),
]


class CountryDeepDiveBriefing(BriefingBase):
    briefing_type = "country_deep_dive"
    title_template = "Country Deep Dive: {country_name} ({country_iso3}) - {date}"
    cadence = "on_demand"

    def __init__(self, country_iso3: str = "USA"):
        super().__init__()
        self.country_iso3 = country_iso3.upper()
        self.methodology_note = (
            "Country deep dive synthesizes all 6 analytical layers: trade (gravity, RCA, "
            "concentration), macro (GDP, inflation, fiscal), labor (wages, employment, "
            "participation), development (convergence, poverty, institutions), agricultural "
            "(food security, supply, prices), and integration (composite score, spillovers). "
            "Each layer score is 0-100 with signal levels: STABLE (0-25), WATCH (25-50), "
            "STRESS (50-75), CRISIS (75-100)."
        )
        self.data_sources = [
            "FRED", "World Bank WDI", "ILO", "UN Comtrade", "BACI",
            "FAOSTAT", "IMF WEO", "Penn World Table", "V-Dem",
        ]

    async def gather_data(self, db, **kwargs) -> dict:
        """Query all 6 layers of analysis for the specified country."""
        country_iso3 = kwargs.get("country_iso3", self.country_iso3)
        data: dict = {
            "country_iso3": country_iso3,
            "country_name": "",
            "layers": {},
            "series_data": {},
            "composite": None,
        }

        # Country info
        country = await db.fetch_one(
            "SELECT * FROM countries WHERE iso3 = ?", (country_iso3,)
        )
        if country:
            data["country_name"] = country.get("name", country_iso3)
        else:
            data["country_name"] = country_iso3

        # Analysis results per layer
        for layer_key, _ in _LAYERS:
            results = await db.fetch_all(
                "SELECT analysis_type, result, score, signal, created_at "
                "FROM analysis_results WHERE country_iso3 = ? AND layer = ? "
                "ORDER BY created_at DESC LIMIT 10",
                (country_iso3, layer_key),
            )
            data["layers"][layer_key] = results

        # Key time-series for this country
        series_list = await db.fetch_all(
            "SELECT id, source, series_id, name, unit FROM data_series "
            "WHERE country_iso3 = ? ORDER BY source",
            (country_iso3,),
        )
        for s in series_list[:50]:  # cap at 50 series
            points = await db.fetch_all(
                "SELECT date, value FROM data_points WHERE series_id = ? "
                "ORDER BY date DESC LIMIT 20",
                (s["id"],),
            )
            data["series_data"][s["series_id"]] = {
                "name": s["name"],
                "source": s["source"],
                "unit": s["unit"],
                "points": list(reversed(points)),
            }

        # Composite score if available
        composite = await db.fetch_one(
            "SELECT * FROM analysis_results WHERE country_iso3 = ? "
            "AND analysis_type = 'composite_score' ORDER BY created_at DESC LIMIT 1",
            (country_iso3,),
        )
        data["composite"] = composite

        return data

    def _signal_color(self, signal: str | None) -> str:
        if signal == "STABLE":
            return "#059669"
        if signal == "WATCH":
            return _AMBER
        if signal == "STRESS":
            return "#ea580c"
        if signal == "CRISIS":
            return "#e11d48"
        return "#64748b"

    def build_sections(self, data: dict) -> None:
        self.sections = []
        country_name = data.get("country_name", data["country_iso3"])
        layers = data.get("layers", {})
        composite = data.get("composite")

        # Executive Summary
        if composite:
            score = composite.get("score", 0) or 0
            signal = composite.get("signal", "N/A")
            self.sections.append({
                "heading": "Executive Summary",
                "body": (
                    f"<p>{country_name} composite economic score is "
                    f'<strong style="color: {self._signal_color(signal)}">{score:.1f}/100 '
                    f"({signal})</strong>. This deep dive examines all six analytical "
                    f"layers to identify strengths, vulnerabilities, and emerging trends.</p>"
                ),
            })
        else:
            self.sections.append({
                "heading": "Executive Summary",
                "body": (
                    f"<p>Comprehensive 6-layer analysis for {country_name}. "
                    f"Composite scoring will be available as layer analyses accumulate.</p>"
                ),
            })

        # Per-layer sections
        for layer_key, layer_label in _LAYERS:
            results = layers.get(layer_key, [])
            if results:
                items = []
                for r in results[:5]:
                    score_str = f'{r["score"]:.1f}' if r.get("score") is not None else "N/A"
                    signal = r.get("signal", "")
                    sig_badge = ""
                    if signal:
                        sig_badge = (
                            f' <span style="display:inline-block; background:'
                            f'{self._signal_color(signal)}; color:#fff; font-size:10px; '
                            f'padding:1px 6px; border-radius:2px;">{signal}</span>'
                        )
                    atype = r["analysis_type"].replace("_", " ").title()
                    items.append(
                        f"<li><strong>{atype}</strong>: score {score_str}{sig_badge}</li>"
                    )
                body = f"<ul style='margin: 0; padding-left: 20px;'>{''.join(items)}</ul>"
            else:
                body = f"<p>No {layer_label.lower()} analysis results available yet for {country_name}.</p>"

            self.sections.append({
                "heading": f"Layer: {layer_label}",
                "body": body,
            })

        # Key Indicators table from series data
        series_data = data.get("series_data", {})
        if series_data:
            rows = ""
            for sid, sinfo in list(series_data.items())[:12]:
                pts = sinfo.get("points", [])
                latest = pts[-1] if pts else None
                val_str = f'{latest["value"]:.2f}' if latest else "N/A"
                date_str = latest["date"] if latest else ""
                unit = sinfo.get("unit") or ""
                rows += (
                    f'<tr>'
                    f'<td style="padding:5px 8px; border-bottom:1px solid #e2e8f0;">'
                    f'{sinfo["name"]}</td>'
                    f'<td style="padding:5px 8px; border-bottom:1px solid #e2e8f0;">'
                    f'{val_str} {unit}</td>'
                    f'<td style="padding:5px 8px; border-bottom:1px solid #e2e8f0; '
                    f'color:#64748b; font-size:12px;">{date_str}</td>'
                    f'</tr>'
                )
            self.sections.append({
                "heading": "Key Indicators",
                "body": (
                    '<table style="width:100%; border-collapse:collapse; font-size:14px;">'
                    '<thead><tr>'
                    '<th style="text-align:left; padding:6px 8px; border-bottom:2px solid #e2e8f0;">Indicator</th>'
                    '<th style="text-align:left; padding:6px 8px; border-bottom:2px solid #e2e8f0;">Latest</th>'
                    '<th style="text-align:left; padding:6px 8px; border-bottom:2px solid #e2e8f0;">Date</th>'
                    f'</tr></thead><tbody>{rows}</tbody></table>'
                ),
            })

        # Cards
        self.cards = []
        if composite and composite.get("score") is not None:
            signal = composite.get("signal", "N/A")
            self.cards.append({
                "label": "Composite Score",
                "value": f'{composite["score"]:.0f}',
                "color": self._signal_color(signal),
                "subtitle": signal,
            })

        # Layer count card
        active_layers = sum(1 for v in layers.values() if v)
        self.cards.append({
            "label": "Active Layers",
            "value": f"{active_layers}/6",
            "color": _ACCENT if active_layers >= 4 else _AMBER,
            "subtitle": "Analytical coverage",
        })

        # Indicators card
        self.cards.append({
            "label": "Indicators",
            "value": str(len(series_data)),
            "color": _ACCENT,
            "subtitle": f"Time series for {data['country_iso3']}",
        })

        # Total analyses
        total_analyses = sum(len(v) for v in layers.values())
        self.cards.append({
            "label": "Analyses",
            "value": str(total_analyses),
            "color": _ACCENT,
            "subtitle": "Results across all layers",
        })

    def build_charts(self, data: dict) -> None:
        self.charts = []
        layers = data.get("layers", {})

        # Layer scores radar-style horizontal bar chart
        layer_scores: list[tuple[str, float]] = []
        for layer_key, layer_label in _LAYERS:
            results = layers.get(layer_key, [])
            scores = [r["score"] for r in results if r.get("score") is not None]
            if scores:
                avg = sum(scores) / len(scores)
                layer_scores.append((layer_label, avg))

        if layer_scores:
            labels = [ls[0] for ls in layer_scores]
            values = [ls[1] for ls in layer_scores]
            colors = [self._signal_color(
                "STABLE" if v < 25 else "WATCH" if v < 50 else "STRESS" if v < 75 else "CRISIS"
            ) for v in values]
            chart_id = "layer_scores"
            self.charts.append(
                f'<div id="{chart_id}" style="width:100%;height:300px;"></div>'
                f'<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>'
                f'<script>'
                f'Plotly.newPlot("{chart_id}", [{{y: {labels}, x: {values}, '
                f'type: "bar", orientation: "h", '
                f'marker: {{color: {colors}}}}}], '
                f'{{title: "Layer Scores (0=Stable, 100=Crisis)", '
                f'margin: {{t: 40, r: 20, b: 40, l: 120}}, '
                f'xaxis: {{range: [0, 100], gridcolor: "#e2e8f0"}}, '
                f'paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", '
                f'font: {{family: "-apple-system, sans-serif", color: "#0f172a"}}}}, '
                f'{{responsive: true}})'
                f'</script>'
            )

        # GDP or first available series as a time-series line chart
        series_data = data.get("series_data", {})
        # Try to find a GDP-related series
        gdp_series = None
        for sid, sinfo in series_data.items():
            name_lower = sinfo["name"].lower()
            if "gdp" in name_lower and sinfo.get("points"):
                gdp_series = sinfo
                break

        # Fallback to first series with data
        if gdp_series is None:
            for sid, sinfo in series_data.items():
                if sinfo.get("points"):
                    gdp_series = sinfo
                    break

        if gdp_series and gdp_series["points"]:
            pts = gdp_series["points"]
            dates = [p["date"] for p in pts]
            values = [p["value"] for p in pts]
            chart_id = "key_indicator_trend"
            series_name = gdp_series["name"]
            self.charts.append(
                f'<div id="{chart_id}" style="width:100%;height:320px;"></div>'
                f'<script>'
                f'Plotly.newPlot("{chart_id}", [{{x: {dates}, y: {values}, '
                f'type: "scatter", mode: "lines+markers", '
                f'line: {{color: "{_ACCENT}", width: 2}}, '
                f'marker: {{size: 5, color: "{_ACCENT}"}}}}], '
                f'{{title: "{series_name}", '
                f'margin: {{t: 40, r: 20, b: 40, l: 60}}, '
                f'paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", '
                f'xaxis: {{gridcolor: "#e2e8f0"}}, yaxis: {{gridcolor: "#e2e8f0"}}, '
                f'font: {{family: "-apple-system, sans-serif", color: "#0f172a"}}}}, '
                f'{{responsive: true}})'
                f'</script>'
            )

    async def generate(self, db, **kwargs) -> dict:
        """Override to inject country_iso3 and country_name into title."""
        country_iso3 = kwargs.get("country_iso3", self.country_iso3)
        self.country_iso3 = country_iso3.upper()

        data = await self.gather_data(db, country_iso3=self.country_iso3)
        self.build_sections(data)
        self.build_charts(data)

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).strftime("%B %d, %Y")
        title = self.title_template.format(
            date=now,
            country_name=data.get("country_name", self.country_iso3),
            country_iso3=self.country_iso3,
        )
        body_html = self.assemble_html()
        return {
            "title": title,
            "briefing_type": self.briefing_type,
            "body_html": body_html,
            "methodology_note": self.methodology_note,
        }

    async def save(self, result: dict, db, country_iso3: str = "") -> int:
        """Override to use the country_iso3 from the instance."""
        iso3 = country_iso3 or self.country_iso3
        return await super().save(result, db, country_iso3=iso3)
