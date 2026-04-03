"""Foreign Aid Effectiveness module.

Measures ODA absorption efficiency: high aid relative to GNI but low GDP growth
signals that aid is failing to translate into economic expansion (Burnside & Dollar
2000; Easterly 2003). Score rises when aid dependency is high and growth is low.

Sources: WDI (DT.ODA.ALLD.GD.ZS, NY.GDP.MKTP.KD.ZG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# ODA/GNI threshold above which a country is aid-dependent
AID_HIGH_THRESHOLD = 5.0  # percent
# Growth rate below which aid is judged ineffective
GROWTH_LOW_THRESHOLD = 2.0  # percent


class ForeignAidEffectiveness(LayerBase):
    layer_id = "lIN"
    name = "Foreign Aid Effectiveness"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        oda_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'DT.ODA.ALLD.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not oda_rows or not gdp_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for ODA or GDP growth",
            }

        oda_values = [float(r["value"]) for r in oda_rows if r["value"] is not None]
        gdp_values = [float(r["value"]) for r in gdp_rows if r["value"] is not None]

        if not oda_values or not gdp_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no valid numeric data",
            }

        latest_oda = oda_values[0]
        avg_growth = float(np.mean(gdp_values))
        avg_oda = float(np.mean(oda_values))

        # Aid effectiveness stress: high ODA dependency + low growth
        # ODA penalty: scaled 0-60 above threshold
        oda_penalty = float(np.clip((avg_oda - AID_HIGH_THRESHOLD) * 6, 0, 60))
        # Growth penalty: scaled 0-40 below threshold
        growth_penalty = float(np.clip((GROWTH_LOW_THRESHOLD - avg_growth) * 8, 0, 40))

        score = float(np.clip(oda_penalty + growth_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "latest_oda_pct_gni": round(latest_oda, 3),
            "avg_oda_pct_gni": round(avg_oda, 3),
            "avg_gdp_growth_pct": round(avg_growth, 3),
            "n_oda_obs": len(oda_values),
            "n_growth_obs": len(gdp_values),
        }
