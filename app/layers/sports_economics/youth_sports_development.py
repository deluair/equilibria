"""Youth sports development: education and health spending as participation proxy.

Public expenditure on education (WDI SE.XPD.TOTL.GD.ZS) and health
(SH.XPD.CHEX.GD.ZS) form the twin fiscal pillars supporting youth sports
participation. Countries investing heavily in both education and health
systems sustain the physical education infrastructure, school sport programs,
and preventive health initiatives from which competitive athletes and
mass participation cultures emerge.

Score: joint education + health spending index. Low combined spend (<6% GDP)
-> STABLE underdeveloped; moderate (6-10%) -> WATCH building foundation;
high (10-14%) -> STRESS resource competition for sports vs academics/health;
very high (>14%) -> CRISIS fiscal overcommitment risk.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class YouthSportsDevelopment(LayerBase):
    layer_id = "lSP"
    name = "Youth Sports Development"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        edu_code = "SE.XPD.TOTL.GD.ZS"
        hlt_code = "SH.XPD.CHEX.GD.ZS"

        edu_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (edu_code, "%education expenditure%"),
        )
        hlt_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (hlt_code, "%health expenditure%"),
        )

        edu_vals = [r["value"] for r in edu_rows if r["value"] is not None]
        hlt_vals = [r["value"] for r in hlt_rows if r["value"] is not None]

        if not edu_vals and not hlt_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for education or health expenditure indicators",
            }

        edu = edu_vals[0] if edu_vals else 0.0
        hlt = hlt_vals[0] if hlt_vals else 0.0
        combined = edu + hlt

        if combined < 6.0:
            score = 5.0 + combined * 2.5
        elif combined < 10.0:
            score = 20.0 + (combined - 6.0) * 7.5
        elif combined < 14.0:
            score = 50.0 + (combined - 10.0) * 6.25
        else:
            score = min(100.0, 75.0 + (combined - 14.0) * 3.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "education_expenditure_gdp_pct": round(edu, 2),
                "health_expenditure_gdp_pct": round(hlt, 2),
                "combined_social_spend_pct": round(combined, 2),
                "n_obs_edu": len(edu_vals),
                "n_obs_hlt": len(hlt_vals),
                "development_tier": (
                    "underdeveloped" if combined < 6.0
                    else "building" if combined < 10.0
                    else "established" if combined < 14.0
                    else "mature"
                ),
            },
        }
