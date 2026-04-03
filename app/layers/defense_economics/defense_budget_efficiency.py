"""Defense budget efficiency: military spending as % GDP vs regional peers.

Uses WDI indicator MS.MIL.XPND.GD.ZS (military expenditure as % of GDP).
A very low share may reflect underfunding; a very high share signals resource
diversion from civilian investment and potential fiscal stress.

NATO benchmark: 2% of GDP. Global average is ~2.2%.
Score: <1% -> low investment (STABLE-low), 1-2% -> normal (STABLE),
2-4% -> elevated (WATCH), 4-6% -> high (STRESS), >6% -> CRISIS burden.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DefenseBudgetEfficiency(LayerBase):
    layer_id = "lDX"
    name = "Defense Budget Efficiency"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "MS.MIL.XPND.GD.ZS"
        name = "military expenditure"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        vals = [row["value"] for row in rows if row["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for MS.MIL.XPND.GD.ZS"}

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Score reflects deviation from efficient 1-2% band
        if latest < 1.0:
            score = 15.0 + latest * 10.0  # underfunded
        elif latest < 2.0:
            score = 20.0  # near-optimal
        elif latest < 4.0:
            score = 20.0 + (latest - 2.0) * 12.5
        elif latest < 6.0:
            score = 45.0 + (latest - 4.0) * 12.5
        else:
            score = min(100.0, 70.0 + (latest - 6.0) * 5.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "military_spending_gdp_pct": round(latest, 3),
                "trend_pct_change": trend,
                "n_obs": len(vals),
                "vs_nato_benchmark_pct": round(latest - 2.0, 3),
                "efficiency_band": (
                    "underfunded" if latest < 1.0
                    else "optimal" if latest < 2.0
                    else "elevated" if latest < 4.0
                    else "high" if latest < 6.0
                    else "crisis-level"
                ),
            },
        }
