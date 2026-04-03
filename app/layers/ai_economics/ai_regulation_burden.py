"""AI regulation burden: regulatory quality impact on tech adoption.

Regulatory quality (World Bank Governance Indicators RQ.EST) captures the
ability of governments to formulate and implement sound policies that permit
and promote private sector development, including technology adoption. Poor
regulatory quality creates friction, uncertainty, and compliance costs that
slow AI diffusion. Excessively heavy-handed regulation -- or regulatory
vacuum with high uncertainty -- both impede AI investment.

Score: very poor regulatory quality -> CRISIS (blocks adoption), good
regulatory quality -> STABLE (enables managed AI diffusion).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AIRegulationBurden(LayerBase):
    layer_id = "lAI"
    name = "AI Regulation Burden"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        rq_code = "RQ.EST"
        gov_code = "GE.EST"

        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rq_code, "%regulatory quality%"),
        )
        gov_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gov_code, "%government effectiveness%"),
        )

        rq_vals = [r["value"] for r in rq_rows if r["value"] is not None]
        gov_vals = [r["value"] for r in gov_rows if r["value"] is not None]

        if not rq_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for regulatory quality RQ.EST",
            }

        # WGI estimates range roughly -2.5 to +2.5
        rq_score = rq_vals[0]
        gov_effectiveness = gov_vals[0] if gov_vals else None

        # Score: WGI scale -2.5 to +2.5, map to 0-100
        # Strong positive -> STABLE; strongly negative -> CRISIS
        if rq_score >= 1.5:
            base = 10.0
        elif rq_score >= 0.5:
            base = 10.0 + (1.5 - rq_score) * 15.0
        elif rq_score >= -0.5:
            base = 25.0 + (0.5 - rq_score) * 25.0
        elif rq_score >= -1.5:
            base = 50.0 + (-0.5 - rq_score) * 20.0
        else:
            base = min(95.0, 70.0 + (-1.5 - rq_score) * 12.5)

        # Government effectiveness confirms or modifies the burden assessment
        if gov_effectiveness is not None:
            if gov_effectiveness >= 1.0:
                base = max(5.0, base - 8.0)
            elif gov_effectiveness <= -1.0:
                base = min(100.0, base + 8.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "regulatory_quality_wgi": round(rq_score, 3),
                "government_effectiveness_wgi": round(gov_effectiveness, 3) if gov_effectiveness is not None else None,
                "n_obs_rq": len(rq_vals),
                "n_obs_gov": len(gov_vals),
                "regulatory_environment": (
                    "enabling" if rq_score >= 0.5
                    else "neutral" if rq_score >= -0.5
                    else "restrictive"
                ),
                "adoption_friction_high": score > 50,
            },
        }
