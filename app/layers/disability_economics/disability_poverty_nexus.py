"""Disability poverty nexus: poverty rate among disabled persons.

Disability and poverty are mutually reinforcing: poverty increases the risk of
acquiring a disability through inadequate nutrition, unsafe work, and limited
healthcare; disability increases poverty risk through employment exclusion and
additional costs. Proxied by the interaction of vulnerable employment
(SL.EMP.VULN.ZS) and the national poverty headcount (SP.POV.NAHC).

Score: low vulnerable + low poverty -> STABLE weak nexus.
High vulnerable + high poverty -> CRISIS entrenched disability-poverty trap.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DisabilityPovertyNexus(LayerBase):
    layer_id = "lDI"
    name = "Disability Poverty Nexus"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        vuln_code = "SL.EMP.VULN.ZS"
        pov_code = "SP.POV.NAHC"

        vuln_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (vuln_code, "%vulnerable employment%"),
        )
        pov_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pov_code, "%poverty headcount%"),
        )

        vuln_vals = [r["value"] for r in vuln_rows if r["value"] is not None]
        pov_vals = [r["value"] for r in pov_rows if r["value"] is not None]

        if not vuln_vals and not pov_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for vulnerable employment or poverty headcount"}

        if vuln_vals and pov_vals:
            vuln = vuln_vals[0]
            pov = pov_vals[0]
            vuln_norm = min(1.0, vuln / 80.0)
            pov_norm = min(1.0, pov / 60.0)
            nexus_index = (vuln_norm * 0.55 + pov_norm * 0.45)
            score = round(nexus_index * 100.0, 2)
            return {
                "score": score,
                "signal": self.classify_signal(score),
                "metrics": {
                    "vulnerable_employment_pct": round(vuln, 2),
                    "poverty_headcount_pct": round(pov, 2),
                    "nexus_index": round(nexus_index, 4),
                    "n_obs_vuln": len(vuln_vals),
                    "n_obs_poverty": len(pov_vals),
                },
            }

        if vuln_vals:
            vuln = vuln_vals[0]
            score = round(min(100.0, vuln * 1.0), 2)
            return {
                "score": score,
                "signal": self.classify_signal(score),
                "metrics": {
                    "vulnerable_employment_pct": round(vuln, 2),
                    "poverty_headcount_pct": None,
                    "n_obs_vuln": len(vuln_vals),
                },
            }

        pov = pov_vals[0]
        score = round(min(100.0, pov * 1.2), 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "vulnerable_employment_pct": None,
                "poverty_headcount_pct": round(pov, 2),
                "n_obs_poverty": len(pov_vals),
            },
        }
