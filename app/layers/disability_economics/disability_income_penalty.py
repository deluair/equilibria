"""Disability income penalty: wage gap for workers with disabilities.

Workers with disabilities earn substantially less than non-disabled peers --
driven by occupational segregation, lower hours, productivity assumptions by
employers, and weaker bargaining power. Proxied using the interaction of
income inequality (SI.POV.GINI) and vulnerable employment share (SL.EMP.VULN.ZS).
High Gini in high-vulnerable-employment economies signals outsized earnings
penalties for marginalised groups including disabled workers.

Score: low Gini + low vulnerable -> STABLE narrow penalty.
High Gini + high vulnerable -> CRISIS severe earnings exclusion.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DisabilityIncomePenalty(LayerBase):
    layer_id = "lDI"
    name = "Disability Income Penalty"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gini_code = "SI.POV.GINI"
        vuln_code = "SL.EMP.VULN.ZS"

        gini_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gini_code, "%Gini%"),
        )
        vuln_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (vuln_code, "%vulnerable employment%"),
        )

        gini_vals = [r["value"] for r in gini_rows if r["value"] is not None]
        vuln_vals = [r["value"] for r in vuln_rows if r["value"] is not None]

        if not gini_vals and not vuln_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for Gini or vulnerable employment"}

        if gini_vals and vuln_vals:
            gini = gini_vals[0]
            vuln = vuln_vals[0]
            # Penalty index: normalise both to [0,1] and average
            # Gini range 0-100 (values in WDI are 0-100 scale)
            gini_norm = min(1.0, gini / 65.0)
            vuln_norm = min(1.0, vuln / 80.0)
            penalty_index = (gini_norm + vuln_norm) / 2.0
            score = round(penalty_index * 100.0, 2)
            return {
                "score": score,
                "signal": self.classify_signal(score),
                "metrics": {
                    "gini_index": round(gini, 2),
                    "vulnerable_employment_pct": round(vuln, 2),
                    "penalty_index": round(penalty_index, 4),
                    "n_obs_gini": len(gini_vals),
                    "n_obs_vuln": len(vuln_vals),
                },
            }

        # Fallback: single-indicator scoring
        if gini_vals:
            gini = gini_vals[0]
            score = min(100.0, gini * 1.4)
            return {
                "score": round(score, 2),
                "signal": self.classify_signal(score),
                "metrics": {
                    "gini_index": round(gini, 2),
                    "vulnerable_employment_pct": None,
                    "n_obs_gini": len(gini_vals),
                },
            }

        vuln = vuln_vals[0]
        score = min(100.0, vuln * 1.0)
        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "gini_index": None,
                "vulnerable_employment_pct": round(vuln, 2),
                "n_obs_vuln": len(vuln_vals),
            },
        }
