"""Supply Chain Disruption Risk module.

Estimates vulnerability to supply chain shocks from two dimensions:

1. **Import volatility** (NE.IMP.GNFS.ZS): high year-to-year variability in
   import volumes signals concentration in few suppliers or exposure to
   commodity price shocks. Measured as coefficient of variation (CV).

2. **Political instability** (PV.EST): World Bank Political Stability / Absence
   of Violence estimate. More negative values indicate higher instability,
   which elevates both internal supply disruption risk and partner risk.

Combined score:
  import_cv_score = clip(import_cv * 200, 0, 50)
  instability_score = clip((-pv_est + 2.5) / 5 * 50, 0, 50)
  total = import_cv_score + instability_score

Sources: World Bank WDI (NE.IMP.GNFS.ZS, PV.EST).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SupplyChainDisruptionRisk(LayerBase):
    layer_id = "lVC"
    name = "Supply Chain Disruption Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        imp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.IMP.GNFS.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        pv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PV.EST'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not imp_rows or len(imp_rows) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient import data"}

        imp_vals = np.array([float(r["value"]) for r in imp_rows])
        mean_imp = float(np.mean(imp_vals))
        std_imp = float(np.std(imp_vals))
        import_cv = std_imp / mean_imp if mean_imp > 0.1 else 0.0

        pv_est = None
        if pv_rows:
            pv_est = float(np.mean([float(r["value"]) for r in pv_rows]))

        import_cv_score = float(np.clip(import_cv * 200.0, 0.0, 50.0))

        if pv_est is not None:
            instability_score = float(np.clip((-pv_est + 2.5) / 5.0 * 50.0, 0.0, 50.0))
        else:
            instability_score = 25.0  # neutral fallback

        score = import_cv_score + instability_score

        return {
            "score": round(score, 1),
            "country": country,
            "import_cv": round(import_cv, 4),
            "import_cv_score": round(import_cv_score, 1),
            "mean_imports_pct_gdp": round(mean_imp, 2),
            "pv_est": round(pv_est, 4) if pv_est is not None else None,
            "instability_score": round(instability_score, 1),
            "n_obs_imports": len(imp_vals),
            "interpretation": (
                "high disruption risk" if score > 60
                else "moderate disruption risk" if score > 35
                else "low disruption risk"
            ),
        }
