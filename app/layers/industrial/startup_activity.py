"""Startup activity: new business density and regulatory barriers to entry.

New business density (IC.BUS.NDNS.ZS) measures new limited-liability firms
registered per 1,000 working-age adults (ages 15-64). It captures
entrepreneurial activity and the depth of the formal private sector. High
density reflects low entry barriers, strong property rights, and vibrant
firm creation (Klapper, Laeven & Rajan 2006).

Business registration cost (IC.REG.COST.PC.ZS) measures the cost of
registering a business as a share of per-capita GNI. High cost proxies
bureaucratic barriers that suppress formal-sector startup activity,
diverting entrepreneurs into informality.

Dual indicator approach:
    1. Density: direct measure (higher = healthier)
    2. Registration cost: barrier proxy (higher cost = lower activity = higher stress)

Score construction:
    - If density available: score = max(0, 100 - density * 10) [low density = high stress]
      bounded at 0 when density >= 10 firms per 1000 adults (OECD norm ~7-10).
    - If only cost proxy: score = clip(cost_pct * 5, 0, 100) [high cost = high stress]
    - If both: weighted average (density 0.7, cost 0.3)

References:
    Klapper, L., Laeven, L. & Rajan, R. (2006). Entry regulation as a barrier
        to entrepreneurship. Journal of Financial Economics 82(3): 591-629.
    World Bank WDI: IC.BUS.NDNS.ZS, IC.REG.COST.PC.ZS.

Indicators:
    IC.BUS.NDNS.ZS  (New business registrations per 1,000 adults, ages 15-64)
    IC.REG.COST.PC.ZS (Cost of business start-up procedures, % of GNI per capita)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class StartupActivity(LayerBase):
    layer_id = "l14"
    name = "Startup Activity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        density_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'IC.BUS.NDNS.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        cost_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'IC.REG.COST.PC.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        has_density = bool(density_rows)
        has_cost = bool(cost_rows)

        if not has_density and not has_cost:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no startup activity data available",
            }

        density_val = None
        cost_val = None
        density_year = None
        cost_year = None

        if has_density:
            density_val = float(density_rows[0]["value"])
            density_year = density_rows[0]["date"]

        if has_cost:
            cost_val = float(cost_rows[0]["value"])
            cost_year = cost_rows[0]["date"]

        # Score components
        score_density = None
        score_cost = None

        if density_val is not None:
            # Low density = high stress. >= 10 per 1000 = minimal stress.
            score_density = float(np.clip(max(0.0, 100.0 - density_val * 10.0), 0.0, 100.0))

        if cost_val is not None:
            # High cost = high stress. cost_pct * 5 capped at 100.
            score_cost = float(np.clip(cost_val * 5.0, 0.0, 100.0))

        if score_density is not None and score_cost is not None:
            score = 0.7 * score_density + 0.3 * score_cost
        elif score_density is not None:
            score = score_density
        else:
            score = score_cost

        return {
            "score": round(score, 2),
            "country": country,
            "new_business_density": round(density_val, 3) if density_val is not None else None,
            "density_year": density_year,
            "registration_cost_pct_gni": round(cost_val, 2) if cost_val is not None else None,
            "cost_year": cost_year,
            "score_density_component": round(score_density, 2) if score_density is not None else None,
            "score_cost_component": round(score_cost, 2) if score_cost is not None else None,
            "activity_tier": (
                "very high" if (density_val or 0) >= 10
                else "high" if (density_val or 0) >= 5
                else "moderate" if (density_val or 0) >= 2
                else "low"
            ),
        }
