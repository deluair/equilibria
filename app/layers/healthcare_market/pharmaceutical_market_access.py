"""Pharmaceutical market access analysis.

Assesses pharmaceutical market access by combining out-of-pocket health
expenditure (a proxy for medicine affordability barriers) with patent
activity (resident patent applications as a proxy for innovation capacity
and potential price pressure from IP protection).

High OOP with low domestic patent activity signals a market dependent on
imported branded medicines with limited generic competition and high
access barriers.

Key references:
    Hoen, E.F.M. et al. (2011). Medicine procurement and the use of
        flexibilities in the Agreement on Trade-Related Aspects of
        Intellectual Property Rights, 2001-2008. Bulletin of WHO, 89.
    World Bank WDI: SH.XPD.OOPC.CH.ZS, IP.PAT.RESD.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PharmaceuticalMarketAccess(LayerBase):
    layer_id = "lHM"
    name = "Pharmaceutical Market Access"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score pharmaceutical market access barriers.

        High OOP + low domestic patent filings -> poor access environment.
        Score rises with access barriers.
        """
        code_oop = "SH.XPD.OOPC.CH.ZS"
        code_pat = "IP.PAT.RESD"

        oop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_oop, f"%{code_oop}%"),
        )
        pat_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code_pat, f"%{code_pat}%"),
        )

        if not oop_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No OOP health expenditure data in DB",
            }

        oop_vals = [float(r["value"]) for r in oop_rows if r["value"] is not None]
        pat_vals = [float(r["value"]) for r in pat_rows if r["value"] is not None]

        if not oop_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid OOP values",
            }

        mean_oop = float(np.mean(oop_vals))
        # OOP stress component: 0-100 scale
        oop_stress = float(np.clip(mean_oop / 100.0, 0, 1))

        if pat_vals:
            mean_pat = float(np.mean(pat_vals))
            # Low patent activity (LICs typically <500/yr) amplifies access barriers
            # Normalize with log scale; 10,000 filings ~= adequate domestic capacity
            pat_norm = float(np.clip(np.log1p(mean_pat) / np.log1p(10000), 0, 1))
            # High patent activity can reduce access through IP barriers; low = dependency
            # Combined: access barrier = OOP stress weighted by low domestic capacity
            access_barrier = oop_stress * (1.0 - 0.4 * pat_norm)
        else:
            access_barrier = oop_stress
            mean_pat = None
            pat_norm = None

        score = float(np.clip(access_barrier * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "mean_oop_pct_che": round(mean_oop, 2),
                "mean_resident_patents": round(mean_pat, 0) if mean_pat is not None else None,
                "patent_capacity_norm": round(pat_norm, 3) if pat_norm is not None else None,
                "oop_n_obs": len(oop_vals),
                "pat_n_obs": len(pat_vals),
            },
        }
