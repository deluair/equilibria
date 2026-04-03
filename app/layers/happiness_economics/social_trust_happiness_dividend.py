"""Social trust happiness dividend: institutional trust correlated with wellbeing.

High institutional trust -- in government, the judiciary, and public services --
is one of the strongest predictors of national life satisfaction (Helliwell et al.,
World Happiness Report). Countries with high governance effectiveness and rule of
law consistently rank at the top of happiness surveys, independent of income.

This module proxies social trust using WDI governance indicators: government
effectiveness (GE.EST) and rule of law (RL.EST), both drawn from the World Bank
Worldwide Governance Indicators (WGI). Scores range from approximately -2.5
(weak) to +2.5 (strong).

Score: strong governance (high trust) -> STABLE, weak governance -> STRESS/CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SocialTrustHappinessDividend(LayerBase):
    layer_id = "lHE"
    name = "Social Trust Happiness Dividend"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        ge_code = "GE.EST"
        rl_code = "RL.EST"

        ge_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (ge_code, "%Government Effectiveness%"),
        )
        rl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (rl_code, "%Rule of Law%"),
        )

        ge_vals = [r["value"] for r in ge_rows if r["value"] is not None]
        rl_vals = [r["value"] for r in rl_rows if r["value"] is not None]

        if not ge_vals and not rl_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for GE.EST or RL.EST governance indicators",
            }

        # Composite trust score: mean of available WGI dimensions
        components = []
        if ge_vals:
            components.append(ge_vals[0])
        if rl_vals:
            components.append(rl_vals[0])

        trust_composite = sum(components) / len(components)

        # WGI range: -2.5 (weakest) to +2.5 (strongest)
        # Map to 0-100 stress scale: high trust = low score (STABLE)
        # Invert: stress = (2.5 - trust_composite) / 5.0 * 100
        normalized_stress = (2.5 - trust_composite) / 5.0 * 100.0
        score = max(0.0, min(100.0, normalized_stress))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "govt_effectiveness_wgi": round(ge_vals[0], 3) if ge_vals else None,
                "rule_of_law_wgi": round(rl_vals[0], 3) if rl_vals else None,
                "trust_composite_wgi": round(trust_composite, 3),
                "trust_tier": (
                    "high"
                    if trust_composite > 1.0
                    else "moderate"
                    if trust_composite > 0.0
                    else "low"
                    if trust_composite > -1.0
                    else "very_low"
                ),
                "n_obs_ge": len(ge_vals),
                "n_obs_rl": len(rl_vals),
            },
        }
