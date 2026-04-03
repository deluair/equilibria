"""Security-economic stability: composite of political stability and GDP correlation.

Security and economic performance are deeply interlinked. Political instability
raises investment risk, disrupts supply chains, and reduces human capital
accumulation. This module uses WDI Political Stability and Absence of
Violence/Terrorism (PV.EST) from the World Governance Indicators, combined
with GDP growth to form a composite security-economic stability signal.

WGI Political Stability runs from approximately -3 (most unstable) to +3 (most stable).
Globally, the mean is near 0.

Score: high stability + positive GDP growth -> STABLE,
low stability + GDP contraction -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SecurityEconomicStability(LayerBase):
    layer_id = "lDX"
    name = "Security Economic Stability"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        pv_code = "PV.EST"  # Political Stability and Absence of Violence
        gdp_code = "NY.GDP.MKTP.KD.ZG"

        pv_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (pv_code, "%political stability%violence%"),
        )
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (gdp_code, "%GDP growth%annual%"),
        )

        pv_vals = [r["value"] for r in pv_rows if r["value"] is not None]
        gdp_vals = [r["value"] for r in gdp_rows if r["value"] is not None]

        if not pv_vals and not gdp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for political stability PV.EST or GDP growth",
            }

        pv = pv_vals[0] if pv_vals else 0.0  # neutral fallback
        gdp_growth = gdp_vals[0] if gdp_vals else 2.0  # global avg fallback

        # Convert WGI PV (-3 to +3) to stress: higher PV = lower stress
        pv_stress = (3.0 - pv) / 6.0 * 70.0  # maps -3->70, +3->0

        # GDP growth adjustment: contraction adds stress, strong growth reduces it
        gdp_adj = max(-15.0, min(15.0, -gdp_growth * 2.0))

        score = max(0.0, min(100.0, pv_stress + gdp_adj))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "political_stability_wgi": round(pv, 3),
                "gdp_growth_pct": round(gdp_growth, 3),
                "pv_stress_component": round(pv_stress, 3),
                "gdp_adjustment": round(gdp_adj, 3),
                "n_obs_pv": len(pv_vals),
                "n_obs_gdp": len(gdp_vals),
                "stability_level": (
                    "high" if pv > 1.0
                    else "moderate" if pv > 0.0
                    else "low" if pv > -1.0
                    else "fragile"
                ),
            },
        }
