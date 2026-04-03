"""GVC Participation module.

Measures a country's integration into global value chains using trade structure:

1. **Manufacturing exports share** (TX.VAL.MANF.ZS.UN): share of manufactured goods
   in merchandise exports. High share indicates active GVC participation as manufacturer.

2. **Import penetration** (NE.IMP.GNFS.ZS): imports as % GDP. High imports alongside
   low manufacturing exports signals raw-material or final-demand positioning, not GVC
   integration.

Combined measure: low manufacturing exports share = low GVC participation.
Declining in both = GVC exit signal.

Score = clip(max(0, 40 - manf_exports_share) * 1.5, 0, 100).
Higher score = weaker GVC participation = more stress.

Sources: World Bank WDI (TX.VAL.MANF.ZS.UN, NE.IMP.GNFS.ZS).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GVCParticipation(LayerBase):
    layer_id = "lVC"
    name = "GVC Participation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        manf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.MANF.ZS.UN'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        imp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.IMP.GNFS.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not manf_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no manufacturing export data"}

        manf_vals = np.array([float(r["value"]) for r in manf_rows])
        manf_exports_share = float(np.mean(manf_vals))

        imp_vals = np.array([float(r["value"]) for r in imp_rows]) if imp_rows else None
        imports_pct_gdp = float(np.mean(imp_vals)) if imp_vals is not None else None

        # Declining trend check
        gvc_exit = False
        if len(manf_vals) >= 4:
            recent = float(np.mean(manf_vals[:3]))
            older = float(np.mean(manf_vals[3:]))
            if recent < older and imports_pct_gdp is not None and imports_pct_gdp < 25:
                gvc_exit = True

        # Score: low manufacturing exports = low GVC participation = higher stress
        score = float(np.clip(max(0.0, 40.0 - manf_exports_share) * 1.5, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "manf_exports_share_pct": round(manf_exports_share, 2),
            "imports_pct_gdp": round(imports_pct_gdp, 2) if imports_pct_gdp is not None else None,
            "gvc_exit_signal": gvc_exit,
            "n_obs_manf": len(manf_vals),
            "interpretation": (
                "low GVC participation" if manf_exports_share < 20
                else "moderate GVC participation" if manf_exports_share < 50
                else "high GVC participation"
            ),
        }
