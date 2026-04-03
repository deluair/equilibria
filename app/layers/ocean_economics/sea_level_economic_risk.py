"""Sea level economic risk: exposure of coastal populations and GDP to sea-level rise.

Combines disaster risk frequency (EN.CLC.MDAT.ZS), population density (EN.POP.DNST),
and GDP per capita (NY.GDP.PCAP.KD) to estimate the economic exposure of coastal
zones to accelerating sea-level rise. Denser, poorer coastal populations face higher
uncompensated risk.

Sources: World Bank WDI (EN.CLC.MDAT.ZS, EN.POP.DNST, NY.GDP.PCAP.KD), IPCC AR6 SLR
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SeaLevelEconomicRisk(LayerBase):
    layer_id = "lOE"
    name = "Sea Level Economic Risk"

    async def compute(self, db, **kwargs) -> dict:
        disaster_code = "EN.CLC.MDAT.ZS"
        disaster_name = "affected by natural disasters"
        disaster_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (disaster_code, f"%{disaster_name}%"),
        )

        density_code = "EN.POP.DNST"
        density_name = "population density"
        density_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (density_code, f"%{density_name}%"),
        )

        gdp_code = "NY.GDP.PCAP.KD"
        gdp_name = "GDP per capita"
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gdp_code, f"%{gdp_name}%"),
        )

        disaster_vals = [row["value"] for row in disaster_rows if row["value"] is not None]
        density_vals = [row["value"] for row in density_rows if row["value"] is not None]
        gdp_vals = [row["value"] for row in gdp_rows if row["value"] is not None]

        if not disaster_vals and not density_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No sea level risk proxy data found",
            }

        # Disaster exposure component: higher % affected = higher risk
        disaster_latest = float(disaster_vals[0]) if disaster_vals else None
        # Normalize: 0% = no risk, 10%+ = high risk
        disaster_risk = min(disaster_latest / 10.0, 1.0) if disaster_latest is not None else 0.5

        # Density component: denser coastal populations face higher aggregate loss
        density_latest = float(density_vals[0]) if density_vals else None
        # Normalize: 0 ppl/km2 = 0, 1000+ ppl/km2 = max
        density_risk = min(density_latest / 1000.0, 1.0) if density_latest is not None else 0.5

        # Income component: lower GDP per capita = less adaptive capacity
        gdp_latest = float(gdp_vals[0]) if gdp_vals else None
        # Inverse: richer = lower risk. $30k+ = low risk
        income_risk = max(0.0, 1.0 - gdp_latest / 30000.0) if gdp_latest is not None else 0.5

        # Trend in disaster frequency
        trend = "stable"
        if len(disaster_vals) >= 3:
            recent = sum(disaster_vals[:3]) / 3
            older = sum(disaster_vals[-3:]) / 3
            if recent > older * 1.05:
                trend = "worsening"
            elif recent < older * 0.95:
                trend = "improving"

        # Composite: disaster exposure 40%, density 30%, income vulnerability 30%
        score = round(
            min(100.0, disaster_risk * 40.0 + density_risk * 30.0 + income_risk * 30.0), 2
        )

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "disaster_affected_pct": round(disaster_latest, 3) if disaster_latest is not None else None,
                "population_density_per_km2": round(density_latest, 2) if density_latest is not None else None,
                "gdp_per_capita_usd": round(gdp_latest, 0) if gdp_latest is not None else None,
                "disaster_trend": trend,
                "n_disaster_obs": len(disaster_vals),
                "n_density_obs": len(density_vals),
                "n_gdp_obs": len(gdp_vals),
            },
        }
