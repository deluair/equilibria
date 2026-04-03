"""Digital GVC module.

Assesses digital services trade participation and identifies gaps between
digital infrastructure readiness and actual digital export performance.

Two dimensions:

1. **Digital services export share**: services exports as % of total goods+services
   exports. Uses TX.VAL.SERV.ZS.WT (commercial services exports % of total exports)
   or BX.GSR.TOTL.CD (services exports, BoP, current USD) as fallback.

2. **Internet penetration** (IT.NET.USER.ZS): % of population using internet.
   High internet + low digital exports = digital GVC gap (country has the
   infrastructure to participate but does not).

Score construction:
  digital_export_score = clip((30 - digital_pct) * 2.5, 0, 50)
                         -- penalizes low digital export share (max stress=75%)
  gap_bonus = clip((internet_pct - digital_pct * 2) / 100 * 30, 0, 30)
               -- extra stress if internet is high but digital exports are low
  total = digital_export_score + gap_bonus

Sources: World Bank WDI (TX.VAL.SERV.ZS.WT or BX.GSR.TOTL.CD, IT.NET.USER.ZS).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DigitalGVC(LayerBase):
    layer_id = "lVC"
    name = "Digital GVC"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # Prefer the WDI percentage indicator; fallback is handled later
        serv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.SERV.ZS.WT'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        # Fallback: absolute USD services exports
        if not serv_rows:
            serv_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = 'BX.GSR.TOTL.CD'
                ORDER BY dp.date DESC
                LIMIT 10
                """,
                (country,),
            )
            serv_as_pct = False
        else:
            serv_as_pct = True

        inet_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not serv_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no services export data"}

        serv_vals = np.array([float(r["value"]) for r in serv_rows])

        if serv_as_pct:
            digital_pct = float(np.mean(serv_vals))
        else:
            # Cannot compute share without total exports; use rank proxy (cap at 50)
            # Scale to 0-50 range based on magnitude (rough)
            digital_pct = min(50.0, float(np.mean(serv_vals)) / 1e10)

        internet_pct = float(np.mean([float(r["value"]) for r in inet_rows])) if inet_rows else None

        digital_export_score = float(np.clip((30.0 - digital_pct) * 2.5, 0.0, 75.0))

        gap_bonus = 0.0
        gap_ppt = None
        if internet_pct is not None:
            gap_ppt = max(0.0, internet_pct - digital_pct * 2.0)
            gap_bonus = float(np.clip(gap_ppt / 100.0 * 30.0, 0.0, 30.0))

        score = float(np.clip(digital_export_score + gap_bonus, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "digital_exports_pct": round(digital_pct, 2),
            "internet_penetration_pct": round(internet_pct, 2) if internet_pct is not None else None,
            "digital_gvc_gap_ppt": round(gap_ppt, 2) if gap_ppt is not None else None,
            "digital_export_score": round(digital_export_score, 1),
            "gap_bonus_score": round(gap_bonus, 1),
            "series_used": "TX.VAL.SERV.ZS.WT" if serv_as_pct else "BX.GSR.TOTL.CD",
            "n_obs_services": len(serv_vals),
            "interpretation": (
                "strong digital GVC participation" if digital_pct > 25
                else "moderate digital GVC presence" if digital_pct > 12
                else "weak digital GVC participation"
            ),
        }
