"""Redistribution Analysis module.

Evaluates fiscal redistribution effectiveness by comparing government
expenditure as a share of GDP against income inequality (Gini). High public
spending combined with persistently high Gini signals redistribution failure.

Indicators:
  - GC.XPN.TOTL.GD.ZS : government expenditure (% of GDP)
  - SI.POV.GINI        : Gini coefficient

Score reflects spend-gini tension: generous spending with poor redistribution
outcome raises the stress score.

Sources: WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Threshold: spending above this share of GDP considered "substantial"
_SPEND_THRESHOLD = 15.0
# Gini above this level considered "unequal" despite spending
_GINI_THRESHOLD = 35.0


class RedistributionAnalysis(LayerBase):
    layer_id = "lWE"
    name = "Redistribution Analysis"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        spend_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        gini_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        if not spend_rows and not gini_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no fiscal or inequality data available",
            }

        spend = float(spend_rows[0]["value"]) if spend_rows else None
        spend_date = spend_rows[0]["date"] if spend_rows else None
        gini = float(gini_rows[0]["value"]) if gini_rows else None
        gini_date = gini_rows[0]["date"] if gini_rows else None

        # --- Score construction ---
        # Base stress from inequality level
        gini_penalty = float(np.clip((gini - _GINI_THRESHOLD) * 1.5, 0, 60)) if gini is not None else 30.0

        # If spending is high but Gini is also high, redistribution is failing -> amplify
        if spend is not None and gini is not None:
            spend_above = max(0.0, spend - _SPEND_THRESHOLD)
            # Efficiency gap: spending exists but Gini persists -> extra penalty
            efficiency_gap = float(np.clip(spend_above * (gini / 100) * 2.0, 0, 40))
        elif gini is not None:
            efficiency_gap = 0.0
        else:
            efficiency_gap = 0.0

        score = float(np.clip(gini_penalty + efficiency_gap, 0, 100))

        # Redistribution effectiveness ratio
        redir_ratio = None
        if spend is not None and gini is not None and spend > 0:
            # Higher ratio = better redistribution
            redir_ratio = round((100 - gini) / spend, 4)

        return {
            "score": round(score, 1),
            "country": country,
            "gov_expenditure_pct_gdp": round(spend, 2) if spend is not None else None,
            "spend_date": spend_date,
            "gini": round(gini, 2) if gini is not None else None,
            "gini_date": gini_date,
            "redistribution_efficiency_ratio": redir_ratio,
            "gini_penalty": round(gini_penalty, 2),
            "efficiency_gap_penalty": round(efficiency_gap, 2),
            "method": "Gini stress + efficiency gap from spend-Gini tension",
            "reference": "Lindert 2004; Atkinson 2015; IMF Fiscal Monitor",
        }
