"""Property Protection Gap module.

Proxies intellectual property (IP) protection strength using two signals:
1. IP.PAT.RESD: Patent applications by residents (World Bank). Higher resident
   patent activity signals a functioning IP system and innovation incentives.
   Very low patent activity relative to population may reflect weak IP protection.
2. RL.EST: Rule of Law (WGI). Strong rule of law underpins IP enforcement.

The gap is framed as: weak IP ecosystem = low patent density + weak rule of law.
Score reflects how much the IP protection system falls short of supporting
innovation-driven growth (Romer 1990, Helpman 1993).

References:
    World Bank. (2023). IP.PAT.RESD; RL.EST.
    Helpman, E. (1993). Innovation, Imitation, and Intellectual Property Rights. Em 61(6).
    Romer, P.M. (1990). Endogenous Technological Change. JPE 98(5).
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase


class PropertyProtectionGap(LayerBase):
    layer_id = "lIE"
    name = "Property Protection Gap"

    async def compute(self, db, **kwargs) -> dict:
        pat_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("IP.PAT.RESD", "%patent applications%resident%"),
        )
        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("RL.EST", "%rule of law%"),
        )

        if not pat_rows and not rl_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no IP protection data"}

        metrics = {}
        stresses = []

        if pat_rows:
            pat = float(pat_rows[0]["value"])
            # Log-scale stress: 0 patents = max stress; 50000+ = low stress
            # Use log10(patents+1) normalized against ~50k upper reference
            log_pat = math.log10(pat + 1)
            log_ref = math.log10(50001)
            pat_stress = 1.0 - min(log_pat / log_ref, 1.0)
            stresses.append(pat_stress)
            metrics["patent_applications_residents"] = round(pat, 0)
            metrics["patent_stress"] = round(pat_stress, 4)

        if rl_rows:
            rl = float(rl_rows[0]["value"])
            rl_stress = 1.0 - (rl + 2.5) / 5.0
            rl_stress = max(0.0, min(1.0, rl_stress))
            stresses.append(rl_stress)
            metrics["rl_est"] = round(rl, 4)
            metrics["rl_stress"] = round(rl_stress, 4)

        composite_stress = sum(stresses) / len(stresses)
        score = round(composite_stress * 100.0, 2)
        metrics["n_indicators"] = len(stresses)

        return {
            "score": score,
            "metrics": metrics,
            "reference": "WB IP.PAT.RESD + RL.EST; Helpman 1993; Romer 1990 JPE",
        }
