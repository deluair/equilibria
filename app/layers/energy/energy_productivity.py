"""Energy productivity: GDP output per unit of energy consumed.

Queries World Bank WDI series EG.GDP.PUSE.KO.PP.KD (GDP per unit of energy
use, constant 2017 PPP $ per kg of oil equivalent). Low GDP per kg oil
equivalent signals energy inefficiency -- more energy is needed to produce
the same economic output.

Score = clip(max(0, 10 - gdp_per_kg_oil) * 10, 0, 100):
  - GDP/kg = 0   -> score 100 (maximum inefficiency)
  - GDP/kg = 5   -> score 50
  - GDP/kg >= 10 -> score 0  (high-productivity, efficient economy)

Sources: World Bank WDI (EG.GDP.PUSE.KO.PP.KD)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class EnergyProductivity(LayerBase):
    layer_id = "l16"
    name = "Energy Productivity"
    weight = 0.20

    # Reference level: above this productivity -> no stress
    PRODUCTIVITY_CEILING = 10.0  # PPP $/kg oil eq

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3")

        if not country:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "country_iso3 required",
            }

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'EG.GDP.PUSE.KO.PP.KD'
              AND ds.country_iso3 = ?
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no energy productivity data",
            }

        valid = [(r["date"][:4], float(r["value"])) for r in rows if r["value"] is not None]

        if not valid:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all energy productivity values are null",
            }

        latest_year, gdp_per_kg_oil = valid[-1]

        score = float(np.clip(max(0.0, self.PRODUCTIVITY_CEILING - gdp_per_kg_oil) * 10, 0, 100))

        trend = None
        if len(valid) >= 5:
            yrs = np.array([float(y) for y, _ in valid])
            vals = np.array([v for _, v in valid])
            slope, _, r_value, p_value, _ = linregress(yrs, vals)
            trend = {
                "slope_per_year": round(float(slope), 4),
                "r_squared": round(float(r_value ** 2), 4),
                "p_value": round(float(p_value), 4),
                "direction": (
                    "improving" if slope > 0.05 and p_value < 0.10
                    else "worsening" if slope < -0.05 and p_value < 0.10
                    else "stable"
                ),
            }

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "series_id": "EG.GDP.PUSE.KO.PP.KD",
                "latest_year": latest_year,
                "gdp_per_kg_oil_ppp_usd": round(gdp_per_kg_oil, 3),
                "productivity_ceiling": self.PRODUCTIVITY_CEILING,
                "gap_to_ceiling": round(max(0.0, self.PRODUCTIVITY_CEILING - gdp_per_kg_oil), 3),
                "n_obs": len(valid),
                "trend": trend,
                "low_productivity": gdp_per_kg_oil < 5.0,
            },
        }
