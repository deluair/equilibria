"""Healthcare aging cost: elderly share x health expenditure pressure.

As populations age, healthcare costs rise nonlinearly. Per-capita health
spending follows a J-curve with age, accelerating sharply post-65. This
module estimates the compound pressure from elderly population share and
current health expenditure as a share of GDP.

Score: joint high elderly share + high health spend -> CRISIS fiscal overload.
Low elderly share with moderate health spend -> STABLE manageable cost curve.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class HealthcareAgingCost(LayerBase):
    layer_id = "lAG"
    name = "Healthcare Aging Cost"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        pop_code = "SP.POP.65UP.TO.ZS"
        he_code = "SH.XPD.CHEX.GD.ZS"

        pop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pop_code, "%Population ages 65%"),
        )
        he_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (he_code, "%health expenditure%"),
        )

        pop_vals = [r["value"] for r in pop_rows if r["value"] is not None]
        he_vals = [r["value"] for r in he_rows if r["value"] is not None]

        if not pop_vals or not he_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for elderly share or health expenditure",
            }

        elderly_share = pop_vals[0]
        health_exp_gdp = he_vals[0]

        # Composite cost index: elderly share (%) * health_exp (% GDP) / 100
        # Normalized to 0-100 scale. Reference: ~3 (low) to ~20+ (crisis)
        cost_index = elderly_share * health_exp_gdp / 100.0

        # Score mapping: cost_index thresholds
        if cost_index < 0.5:
            score = 10.0 + cost_index * 20.0
        elif cost_index < 1.5:
            score = 20.0 + (cost_index - 0.5) * 25.0
        elif cost_index < 3.0:
            score = 45.0 + (cost_index - 1.5) * 20.0
        else:
            score = min(100.0, 75.0 + (cost_index - 3.0) * 5.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "elderly_share_pct": round(elderly_share, 2),
                "health_expenditure_gdp_pct": round(health_exp_gdp, 2),
                "cost_index": round(cost_index, 4),
                "n_obs_pop": len(pop_vals),
                "n_obs_health": len(he_vals),
            },
        }
