"""Government effectiveness from WGI indicator.

The World Bank Worldwide Governance Indicators (WGI) Government Effectiveness
dimension captures perceptions of the quality of public services, the civil
service, policy formulation, and the government's credibility. Scores range
from approximately -2.5 (worst) to +2.5 (best).

This module converts the WGI GE.EST score to a stress score: countries
with high governance quality (positive GE) score low on stress; countries
with negative GE (weak governance) score high.

Formula: score = clip(50 - ge_score * 20, 0, 100).
  At GE = +2.5 (best): score = max(0, 50 - 50) = 0.
  At GE = 0 (median): score = 50.
  At GE = -2.5 (worst): score = min(100, 50 + 50) = 100.

High score = low government effectiveness = high governance stress.

References:
    Kaufmann, D., Kraay, A. & Mastruzzi, M. (2010). The Worldwide Governance
        Indicators: Methodology and Analytical Issues. World Bank Policy
        Research WP 5430.
    World Bank WGI: https://info.worldbank.org/governance/wgi/

Sources: WDI 'GE.EST' (WGI Government Effectiveness Estimate).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class GovernmentEffectiveness(LayerBase):
    layer_id = "l10"
    name = "Government Effectiveness"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score governance stress from WGI Government Effectiveness.

        GE.EST ranges from ~-2.5 to +2.5. Converted to 0-100 stress scale.
        """
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GE.EST'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no WGI government effectiveness data (GE.EST)",
            }

        latest = rows[0]
        ge_score = float(latest["value"])
        year = latest["date"][:4]

        score = float(min(max(50.0 - ge_score * 20.0, 0.0), 100.0))

        governance_tier = (
            "strong" if ge_score >= 1.0
            else "adequate" if ge_score >= 0.0
            else "weak" if ge_score >= -1.0
            else "fragile"
        )

        return {
            "score": score,
            "results": {
                "country": country,
                "year": year,
                "ge_estimate": ge_score,
                "wgi_scale": "-2.5 (worst) to +2.5 (best)",
                "governance_tier": governance_tier,
                "percentile_approx": float(min(max((ge_score + 2.5) / 5.0 * 100, 0), 100)),
            },
        }
