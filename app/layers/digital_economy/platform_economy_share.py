"""Platform Economy Share module.

Gig and platform workers as a share of total labor force.
Proxy: SL.EMP.SELF.ZS (self-employed % of total employment, WDI) as a structural
upper-bound proxy for non-standard work arrangements including platform labor.

Score: higher platform/self-employment share without social protection = higher risk.

Source: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PlatformEconomyShare(LayerBase):
    layer_id = "lDG"
    name = "Platform Economy Share"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        self_emp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.EMP.SELF.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        vuln_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.EMP.VULN.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not self_emp_rows and not vuln_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no self-employment data"}

        self_emp_vals = [float(r["value"]) for r in self_emp_rows if r["value"] is not None]
        vuln_vals = [float(r["value"]) for r in vuln_rows if r["value"] is not None]

        self_emp_mean = float(np.nanmean(self_emp_vals)) if self_emp_vals else None
        vuln_mean = float(np.nanmean(vuln_vals)) if vuln_vals else None

        components, weights = [], []
        if self_emp_mean is not None:
            components.append(float(np.clip(self_emp_mean, 0, 100)))
            weights.append(0.6)
        if vuln_mean is not None:
            components.append(float(np.clip(vuln_mean, 0, 100)))
            weights.append(0.4)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        share = sum(c * w for c, w in zip(components, weights)) / total_w
        score = float(np.clip(share, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "self_employment_pct": round(self_emp_mean, 2) if self_emp_mean is not None else None,
            "vulnerable_employment_pct": round(vuln_mean, 2) if vuln_mean is not None else None,
            "note": "Higher score = larger non-standard/platform labor share (risk indicator).",
            "_citation": "World Bank WDI: SL.EMP.SELF.ZS, SL.EMP.VULN.ZS",
        }
