"""Manufacturing value added as share of GDP: deindustrialization and structural change.

Manufacturing value added (MVA) as a percentage of GDP measures industrial
deepening. For low- and middle-income countries, rising MVA signals structural
transformation and productivity gains (Rodrik 2016). Declining MVA in countries
below the manufacturing peak (typically 25-30% of GDP) signals premature
deindustrialization, a development trap associated with stagnating wages
and persistent informality.

Benchmark thresholds (World Bank cross-country evidence):
    MVA < 10%:   structural gap -- agriculture-dependent or de-industrializing
    10-15%:      early-stage manufacturing
    15-25%:      industrializing
    25-30%:      manufacturing peak (East Asian success norm)
    > 30%:       post-peak or re-industrializing

Trend slope matters as much as level. A country at 14% and declining faces
worse prospects than one at 12% and rising.

Score formula (as specified):
    score = clip(max(0, 15 - latest) * 4 - slope * 100, 0, 100)
    where slope is the annualized OLS slope in percentage-point units.

References:
    Rodrik, D. (2016). Premature deindustrialization. JEG 21(1): 1-11.
    UNIDO (2022). International Yearbook of Industrial Statistics.
    World Bank WDI: NV.IND.MANF.ZS.

Indicator: NV.IND.MANF.ZS (Manufacturing, value added, % of GDP).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class ManufacturingValueAdded(LayerBase):
    layer_id = "l14"
    name = "Manufacturing Value Added"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'NV.IND.MANF.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient manufacturing value added data",
            }

        dates = [r["date"] for r in rows]
        values = np.array([float(r["value"]) for r in rows], dtype=float)

        latest = float(values[-1])
        t = np.arange(len(values), dtype=float)
        slope, intercept, r_value, p_value, se = linregress(t, values)

        # Score: low MVA and declining = high stress
        score = float(np.clip(max(0.0, 15.0 - latest) * 4.0 - slope * 100.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "latest_pct": round(latest, 2),
            "latest_year": dates[-1],
            "slope_pp_per_year": round(float(slope), 4),
            "r_squared": round(float(r_value ** 2), 4),
            "p_value": round(float(p_value), 4),
            "n_obs": len(values),
            "mean_pct": round(float(np.mean(values)), 2),
            "trend_direction": "declining" if slope < 0 else "rising",
            "classification": (
                "structural gap" if latest < 10
                else "early-stage manufacturing" if latest < 15
                else "industrializing" if latest < 25
                else "manufacturing peak" if latest < 30
                else "post-peak"
            ),
        }
