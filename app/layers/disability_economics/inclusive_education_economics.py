"""Inclusive education economics: special needs education investment gap.

Children with disabilities have a right to inclusive education, yet many
countries under-invest in special education teachers, adaptive curricula,
and accessible school infrastructure. Proxied by total education expenditure
as a share of GDP (SE.XPD.TOTL.GD.ZS): low spending signals an inadequate
base from which inclusive programs cannot be funded.

Score: high education spend -> STABLE sufficient base for inclusive programs.
Very low education spend -> CRISIS inadequate foundation for inclusive education.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class InclusiveEducationEconomics(LayerBase):
    layer_id = "lDI"
    name = "Inclusive Education Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        edu_code = "SE.XPD.TOTL.GD.ZS"

        edu_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (edu_code, "%education expenditure%"),
        )

        edu_vals = [r["value"] for r in edu_rows if r["value"] is not None]

        if not edu_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SE.XPD.TOTL.GD.ZS"}

        edu_gdp = edu_vals[0]
        trend = round(edu_vals[0] - edu_vals[-1], 3) if len(edu_vals) > 1 else None

        # UNESCO benchmark: 4-6% of GDP for adequate education system
        # Below 2% -> severe gap; 2-4% -> moderate; 4-6% -> adequate; 6%+ -> strong
        if edu_gdp >= 6.0:
            score = 8.0
        elif edu_gdp >= 4.0:
            score = 8.0 + (6.0 - edu_gdp) * 6.5
        elif edu_gdp >= 2.0:
            score = 21.0 + (4.0 - edu_gdp) * 12.0
        else:
            score = min(100.0, 45.0 + (2.0 - edu_gdp) * 27.5)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "education_expenditure_gdp_pct": round(edu_gdp, 2),
                "trend": trend,
                "investment_tier": (
                    "strong" if edu_gdp >= 6.0
                    else "adequate" if edu_gdp >= 4.0
                    else "moderate" if edu_gdp >= 2.0
                    else "critical_gap"
                ),
                "n_obs": len(edu_vals),
            },
        }
