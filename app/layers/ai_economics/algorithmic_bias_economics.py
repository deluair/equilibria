"""Algorithmic bias economics: inequality correlates with algorithmic adoption gaps.

Algorithmic systems trained on historically biased data can amplify existing
inequalities in hiring, credit, healthcare, and criminal justice. Economies
with high gender inequality and income inequality are more vulnerable: biased
algorithms embed existing disparities into automated decisions, creating feedback
loops that are harder to reverse than human-decision bias.

Obermeyer et al. (2019, Science): commercial healthcare algorithm systematically
underestimated needs of Black patients. Gender pay gap correlates with algorithmic
hiring bias (Dastin 2018 Amazon case study).

Score: high income Gini + high gender inequality -> CRISIS (bias amplification risk),
low inequality + strong gender parity -> STABLE.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AlgorithmicBiasEconomics(LayerBase):
    layer_id = "lAI"
    name = "Algorithmic Bias Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gini_code = "SI.POV.GINI"
        gender_code = "SG.GEN.PARL.ZS"

        gini_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gini_code, "%Gini%"),
        )
        gender_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gender_code, "%women in parliament%"),
        )

        gini_vals = [r["value"] for r in gini_rows if r["value"] is not None]
        gender_vals = [r["value"] for r in gender_rows if r["value"] is not None]

        if not gini_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for Gini coefficient SI.POV.GINI",
            }

        gini = gini_vals[0]
        women_parliament = gender_vals[0] if gender_vals else None

        # Base score from income inequality (Gini 0-100)
        if gini < 25:
            base = 10.0
        elif gini < 35:
            base = 10.0 + (gini - 25) * 2.5
        elif gini < 45:
            base = 35.0 + (gini - 35) * 2.5
        elif gini < 55:
            base = 60.0 + (gini - 45) * 2.0
        else:
            base = min(95.0, 80.0 + (gini - 55) * 1.5)

        # Gender representation in parliament as proxy for gender parity
        # Lower women's representation = higher algorithmic bias risk
        if women_parliament is not None:
            if women_parliament >= 40:
                base = max(5.0, base - 12.0)
            elif women_parliament >= 25:
                base = max(5.0, base - 5.0)
            elif women_parliament < 15:
                base = min(100.0, base + 8.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "gini_coefficient": round(gini, 2),
                "women_parliament_pct": round(women_parliament, 2) if women_parliament is not None else None,
                "n_obs_gini": len(gini_vals),
                "n_obs_gender": len(gender_vals),
                "high_bias_amplification_risk": score > 50,
                "gender_parity_adequate": women_parliament is not None and women_parliament >= 30,
            },
        }
