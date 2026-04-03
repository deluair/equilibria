"""Workforce aging productivity: 65+ share impact on GDP growth as TFP proxy.

An aging workforce affects aggregate productivity through multiple channels:
experience accumulation, lower physical productivity, innovation slowdown,
and reduced risk-taking. This module proxies the TFP effect by examining
the relationship between elderly population share and GDP growth rate.

A higher elderly share combined with low GDP growth signals potential
productivity drag from workforce aging.

Score: high elderly share + low growth -> STRESS/CRISIS, elderly share
with sustained growth -> STABLE (experience premium offsets aging drag).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class WorkforceAgingProductivity(LayerBase):
    layer_id = "lAG"
    name = "Workforce Aging Productivity"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        pop_code = "SP.POP.65UP.TO.ZS"
        gdp_code = "NY.GDP.MKTP.KD.ZG"

        pop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pop_code, "%Population ages 65%"),
        )
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gdp_code, "%GDP.*growth%"),
        )

        pop_vals = [r["value"] for r in pop_rows if r["value"] is not None]
        gdp_vals = [r["value"] for r in gdp_rows if r["value"] is not None]

        if not pop_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for elderly population share SP.POP.65UP.TO.ZS",
            }

        elderly_share = pop_vals[0]
        avg_gdp_growth = sum(gdp_vals[:5]) / min(len(gdp_vals), 5) if gdp_vals else None

        # Productivity drag index: high elderly share + low growth = high score
        # Base from elderly share
        if elderly_share < 7:
            base = 15.0
        elif elderly_share < 14:
            base = 25.0 + (elderly_share - 7) * 2.0
        elif elderly_share < 21:
            base = 39.0 + (elderly_share - 14) * 2.5
        else:
            base = min(90.0, 56.5 + (elderly_share - 21) * 2.0)

        # Adjust for GDP growth: low growth amplifies aging drag
        if avg_gdp_growth is not None:
            if avg_gdp_growth < 1.0:
                base = min(100.0, base + 15.0)
            elif avg_gdp_growth < 3.0:
                base = min(100.0, base + 5.0)
            elif avg_gdp_growth > 5.0:
                base = max(0.0, base - 10.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "elderly_share_pct": round(elderly_share, 2),
                "avg_gdp_growth_recent": round(avg_gdp_growth, 2) if avg_gdp_growth is not None else None,
                "n_obs_pop": len(pop_vals),
                "n_obs_gdp": len(gdp_vals),
                "productivity_drag": score > 50,
            },
        }
