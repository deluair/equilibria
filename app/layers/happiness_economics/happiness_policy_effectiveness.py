"""Happiness policy effectiveness: social spending vs life satisfaction outcomes.

Countries that invest more heavily in social protection -- unemployment insurance,
family support, disability benefits, public pensions -- consistently achieve higher
scores on international wellbeing surveys. The relationship is not linear: spending
must be well-targeted to translate into life satisfaction gains. This module
estimates policy effectiveness as the ratio of social protection spending to
demonstrated life outcomes.

Proxies: social protection expenditure as % of GDP (per World Bank) combined with
poverty headcount ratio (SI.POV.NAHC) as an outcome measure. Low poverty despite
moderate spending signals high policy effectiveness; high poverty despite high
spending signals misallocation.

Score: low poverty + moderate-high social spend -> STABLE,
high poverty + low social spend -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class HappinessPolicyEffectiveness(LayerBase):
    layer_id = "lHE"
    name = "Happiness Policy Effectiveness"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        poverty_code = "SI.POV.NAHC"
        sp_code = "per_allsp.cov_pop_tot"  # ILO social protection coverage

        poverty_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (poverty_code, "%poverty headcount%national%"),
        )
        sp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (sp_code, "%social protection%coverage%"),
        )

        poverty_vals = [r["value"] for r in poverty_rows if r["value"] is not None]
        sp_vals = [r["value"] for r in sp_rows if r["value"] is not None]

        if not poverty_vals and not sp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SI.POV.NAHC or social protection coverage",
            }

        score_parts = []

        if poverty_vals:
            poverty_rate = poverty_vals[0]
            # Poverty headcount: <5% -> STABLE, 5-20% -> WATCH, 20-40% -> STRESS, >40% -> CRISIS
            if poverty_rate < 5:
                pov_score = 5.0 + poverty_rate * 1.0
            elif poverty_rate < 20:
                pov_score = 10.0 + (poverty_rate - 5) * 2.0
            elif poverty_rate < 40:
                pov_score = 40.0 + (poverty_rate - 20) * 1.5
            else:
                pov_score = min(100.0, 70.0 + (poverty_rate - 40) * 1.0)
            score_parts.append(pov_score)

        if sp_vals:
            # Higher coverage -> better policy -> lower stress
            coverage = sp_vals[0]
            sp_score = max(5.0, 80.0 - coverage * 0.75)
            score_parts.append(min(100.0, sp_score))

        score = sum(score_parts) / len(score_parts)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "poverty_headcount_pct": round(poverty_vals[0], 2) if poverty_vals else None,
                "social_protection_coverage_pct": round(sp_vals[0], 2) if sp_vals else None,
                "effectiveness_tier": (
                    "high"
                    if score < 25
                    else "moderate"
                    if score < 50
                    else "low"
                    if score < 75
                    else "failing"
                ),
                "n_obs_poverty": len(poverty_vals),
                "n_obs_sp": len(sp_vals),
            },
        }
