"""ADA compliance cost: regulatory compliance cost for accessibility requirements.

Accessibility regulations (ADA in the US; equivalent in other countries) impose
compliance costs on businesses and public entities -- ramps, accessible restrooms,
sign systems, digital accessibility. High business regulation cost environments
(IC.REG.COST.PC.ZS) combined with weak regulatory quality (RQ.EST, WGI)
indicate either excessive compliance burden or inadequate enforcement capacity.

Score: low reg cost + strong governance -> STABLE manageable compliance.
High cost + weak governance -> CRISIS either prohibitive burden or under-enforcement.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ADAComplianceCost(LayerBase):
    layer_id = "lDI"
    name = "ADA Compliance Cost"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        reg_code = "IC.REG.COST.PC.ZS"
        gov_code = "RQ.EST"

        reg_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (reg_code, "%cost of business start%"),
        )
        gov_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gov_code, "%regulatory quality%"),
        )

        reg_vals = [r["value"] for r in reg_rows if r["value"] is not None]
        gov_vals = [r["value"] for r in gov_rows if r["value"] is not None]

        if not reg_vals and not gov_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for reg cost or governance quality"}

        if reg_vals and gov_vals:
            reg_cost = reg_vals[0]
            gov_est = gov_vals[0]  # WGI: -2.5 (worst) to +2.5 (best)
            # Normalise reg cost: higher = worse (more burden)
            reg_norm = min(1.0, reg_cost / 200.0)
            # Governance: invert so higher = worse (weak governance)
            gov_norm = min(1.0, max(0.0, (2.5 - gov_est) / 5.0))
            composite = (reg_norm * 0.6 + gov_norm * 0.4)
            score = round(composite * 100.0, 2)
            return {
                "score": score,
                "signal": self.classify_signal(score),
                "metrics": {
                    "reg_cost_pct_gni": round(reg_cost, 2),
                    "regulatory_quality_est": round(gov_est, 3),
                    "compliance_burden_index": round(composite, 4),
                    "n_obs_reg": len(reg_vals),
                    "n_obs_gov": len(gov_vals),
                },
            }

        if reg_vals:
            reg_cost = reg_vals[0]
            score = min(100.0, reg_cost * 0.5)
            return {
                "score": round(score, 2),
                "signal": self.classify_signal(score),
                "metrics": {
                    "reg_cost_pct_gni": round(reg_cost, 2),
                    "regulatory_quality_est": None,
                    "n_obs_reg": len(reg_vals),
                },
            }

        gov_est = gov_vals[0]
        # Weak governance = higher score (more problematic)
        gov_norm = min(1.0, max(0.0, (2.5 - gov_est) / 5.0))
        score = round(gov_norm * 100.0, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "reg_cost_pct_gni": None,
                "regulatory_quality_est": round(gov_est, 3),
                "n_obs_gov": len(gov_vals),
            },
        }
