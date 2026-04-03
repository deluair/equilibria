"""Property Rights Index module.

Composite of World Bank WGI Rule of Law (RL.EST) and Regulatory Quality (RQ.EST).
Together these capture both the formal protection of property rights (legal enforcement
and contract sanctity) and the quality of regulations governing economic activity.

Higher WGI estimates indicate stronger governance. Values are rescaled from the
standard WGI range (-2.5 to 2.5) into a 0-100 stress score (higher = more stress).

References:
    World Bank. (2023). Worldwide Governance Indicators.
    North, D.C. (1990). Institutions, Institutional Change and Economic Performance.
    Acemoglu, D. & Johnson, S. (2005). Unbundling Institutions. JPE 113(5).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class PropertyRightsIndex(LayerBase):
    layer_id = "lIE"
    name = "Property Rights Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("RL.EST", "%rule of law%"),
        )
        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("RQ.EST", "%regulatory quality%"),
        )

        if not rl_rows and not rq_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no property rights data"}

        def wgi_to_stress(rows):
            if not rows:
                return 0.5, None
            val = float(rows[0]["value"])
            # WGI: -2.5 (worst) to 2.5 (best) -> stress 0-1 (inverted)
            stress = 1.0 - (val + 2.5) / 5.0
            return max(0.0, min(1.0, stress)), round(val, 4)

        rl_stress, rl_val = wgi_to_stress(rl_rows)
        rq_stress, rq_val = wgi_to_stress(rq_rows)

        n = sum(1 for x in [rl_rows, rq_rows] if x)
        composite_stress = (rl_stress + rq_stress) / n
        score = round(composite_stress * 100.0, 2)

        return {
            "score": score,
            "metrics": {
                "rule_of_law_est": rl_val,
                "regulatory_quality_est": rq_val,
                "rl_stress": round(rl_stress, 4),
                "rq_stress": round(rq_stress, 4),
                "n_indicators": n,
            },
            "reference": "WGI RL.EST + RQ.EST; Acemoglu & Johnson 2005",
        }
