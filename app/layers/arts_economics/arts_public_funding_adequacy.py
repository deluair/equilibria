"""Arts public funding adequacy: government cultural expenditure relative to budget.

Public arts funding sustains cultural institutions, heritage preservation, and
access to the arts for all income groups. Proxied by government expenditure as
% of GDP (GC.XPN.TOTL.GD.ZS) combined with government effectiveness (GE.EST)
as a signal of whether public spending reaches cultural objectives.

Score: low government spending + low effectiveness -> STABLE (small state,
limited arts support), moderate spending with good effectiveness -> WATCH
adequate baseline, high spending with poor effectiveness -> STRESS fiscal
waste, high spending with high effectiveness -> CRISIS budget pressure.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ArtsPublicFundingAdequacy(LayerBase):
    layer_id = "lAR"
    name = "Arts Public Funding Adequacy"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        exp_code = "GC.XPN.TOTL.GD.ZS"
        eff_code = "GE.EST"

        exp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (exp_code, "%Expense%GDP%"),
        )
        eff_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (eff_code, "%Government Effectiveness%"),
        )

        exp_vals = [r["value"] for r in exp_rows if r["value"] is not None]
        eff_vals = [r["value"] for r in eff_rows if r["value"] is not None]

        if not exp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for GC.XPN.TOTL.GD.ZS",
            }

        exp_latest = exp_vals[0]
        # GE.EST ranges approx -2.5 to +2.5; normalize to 0-1
        eff_latest = eff_vals[0] if eff_vals else 0.0
        eff_norm = (eff_latest + 2.5) / 5.0  # 0 = worst, 1 = best

        # Inadequacy score: high spend with low effectiveness = worst outcome
        # Low spend with any effectiveness = also a concern for arts access
        # Reference: OECD avg ~40% GDP, arts sub-share ~0.5-1.5%
        # Score on raw govt expenditure % GDP: <15% = very small state
        if exp_latest < 15.0:
            base_score = 60.0 + (15.0 - exp_latest) * 1.5  # low spending is underfunding stress
        elif exp_latest < 30.0:
            base_score = 35.0 + (30.0 - exp_latest) * 1.67
        elif exp_latest < 45.0:
            base_score = 15.0 + (exp_latest - 30.0) * 1.33
        else:
            base_score = min(100.0, 35.0 + (exp_latest - 45.0) * 1.0)

        # Effectiveness adjustment: good effectiveness reduces stress by up to 10 pts
        score = base_score - (eff_norm * 10.0)
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "govt_expenditure_gdp_pct": round(exp_latest, 2),
                "govt_effectiveness_est": round(eff_latest, 3) if eff_vals else None,
                "effectiveness_norm": round(eff_norm, 3),
                "n_obs_expenditure": len(exp_vals),
                "n_obs_effectiveness": len(eff_vals),
            },
        }
