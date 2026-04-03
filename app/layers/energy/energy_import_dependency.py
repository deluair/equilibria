"""Energy import dependency: net energy imports as % of energy use.

Queries World Bank WDI series EG.IMP.CONS.ZS (energy imports net, % of
energy use). Positive values indicate net import dependence; negative values
indicate net export position. High net imports signal supply security risk and
external vulnerability.

Score = clip(max(0, net_imports) * 0.8, 0, 100):
  - net_imports >= 100% -> score 80 (near-maximum stress)
  - net_imports = 50%   -> score 40
  - net_imports <= 0    -> score 0 (net exporter, no import stress)

Sources: World Bank WDI (EG.IMP.CONS.ZS)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class EnergyImportDependency(LayerBase):
    layer_id = "l16"
    name = "Energy Import Dependency"
    weight = 0.20

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
            WHERE ds.series_id = 'EG.IMP.CONS.ZS'
              AND ds.country_iso3 = ?
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no energy import data",
            }

        valid = [(r["date"][:4], float(r["value"])) for r in rows if r["value"] is not None]

        if not valid:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all energy import values are null",
            }

        latest_year, net_imports = valid[-1]

        score = float(np.clip(max(0.0, net_imports) * 0.8, 0, 100))

        trend = None
        if len(valid) >= 5:
            yrs = np.array([float(y) for y, _ in valid])
            vals = np.array([v for _, v in valid])
            slope, _, r_value, p_value, _ = linregress(yrs, vals)
            trend = {
                "slope_pct_per_year": round(float(slope), 3),
                "r_squared": round(float(r_value ** 2), 4),
                "p_value": round(float(p_value), 4),
                "direction": (
                    "worsening" if slope > 0.2 and p_value < 0.10
                    else "improving" if slope < -0.2 and p_value < 0.10
                    else "stable"
                ),
            }

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "series_id": "EG.IMP.CONS.ZS",
                "latest_year": latest_year,
                "net_imports_pct": round(net_imports, 2),
                "n_obs": len(valid),
                "trend": trend,
                "net_exporter": net_imports < 0,
                "high_import_dependence": net_imports >= 50.0,
            },
        }
