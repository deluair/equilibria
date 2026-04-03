"""Disability Inclusion module.

Disability economic inclusion proxy: vulnerable employment combined with governance quality.

Queries:
- 'SL.EMP.VULN.ZS' (vulnerable employment as % of total employment)
- 'GE.EST' (government effectiveness estimate, World Governance Indicators)

High vulnerable employment + poor governance = disability exclusion stress.

Score = clip(vulnerable_employment * max(0, 1 - governance_norm) * 1.5, 0, 100)

where governance_norm maps GE.EST from [-2.5, 2.5] to [0, 1].

Sources: WDI (SL.EMP.VULN.ZS, GE.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DisabilityInclusion(LayerBase):
    layer_id = "lSP"
    name = "Disability Inclusion"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

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

        gov_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GE.EST'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not vuln_rows or not gov_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        vuln_vals = [float(r["value"]) for r in vuln_rows if r["value"] is not None]
        gov_vals = [float(r["value"]) for r in gov_rows if r["value"] is not None]

        if not vuln_vals or not gov_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        vulnerable_employment = float(np.mean(vuln_vals))
        governance_raw = float(np.mean(gov_vals))

        # Map GE.EST from [-2.5, 2.5] to [0, 1], where 1 = best governance
        governance_norm = float(np.clip((governance_raw + 2.5) / 5.0, 0.0, 1.0))
        governance_gap = max(0.0, 1.0 - governance_norm)

        score = float(np.clip(vulnerable_employment * governance_gap * 1.5, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "vulnerable_employment_pct": round(vulnerable_employment, 2),
            "governance_effectiveness_raw": round(governance_raw, 4),
            "governance_norm": round(governance_norm, 4),
            "governance_gap": round(governance_gap, 4),
            "n_obs_vuln": len(vuln_vals),
            "n_obs_gov": len(gov_vals),
            "interpretation": (
                "High vulnerable employment combined with poor government effectiveness "
                "signals limited economic inclusion for people with disabilities."
            ),
            "_series": ["SL.EMP.VULN.ZS", "GE.EST"],
            "_source": "WDI",
        }
