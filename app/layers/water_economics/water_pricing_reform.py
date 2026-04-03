"""Water pricing reform: regulatory quality enabling efficient water pricing.

Combines RQ.EST (regulatory quality, WGI) with ER.H2O.FWTL.ZS (freshwater
withdrawal %). Countries with poor regulatory quality and high water withdrawal
lack the institutional capacity to implement cost-reflective water pricing,
perpetuating overuse and underinvestment.

Sources: World Bank WGI (RQ.EST), WDI (ER.H2O.FWTL.ZS)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class WaterPricingReform(LayerBase):
    layer_id = "lWA"
    name = "Water Pricing Reform"

    async def compute(self, db, **kwargs) -> dict:
        rq_code = "RQ.EST"
        rq_name = "regulatory quality"
        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rq_code, f"%{rq_name}%"),
        )

        withdrawal_code = "ER.H2O.FWTL.ZS"
        withdrawal_name = "freshwater withdrawals"
        withdrawal_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (withdrawal_code, f"%{withdrawal_name}%"),
        )

        rq_vals = [row["value"] for row in rq_rows if row["value"] is not None]
        withdrawal_vals = [row["value"] for row in withdrawal_rows if row["value"] is not None]

        if not rq_vals and not withdrawal_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No regulatory quality or withdrawal data found",
            }

        # RQ.EST: -2.5 to +2.5; higher = better regulatory environment
        rq_latest = float(rq_vals[0]) if rq_vals else 0.0
        # Normalize: -2.5 -> high reform gap, +2.5 -> low reform gap
        reform_gap = min(max((0.0 - rq_latest) / 2.5 * 0.5 + 0.5, 0.0), 1.0)

        withdrawal_latest = float(withdrawal_vals[0]) if withdrawal_vals else 25.0
        water_pressure = min(withdrawal_latest / 100.0, 1.0)

        # Reform urgency: weak regulation + high withdrawal = high risk
        score = round(min(100.0, (reform_gap * 0.6 + water_pressure * 0.4) * 100.0), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "regulatory_quality_est": round(rq_latest, 3) if rq_vals else None,
                "reform_gap_norm": round(reform_gap, 3),
                "withdrawal_pct": round(withdrawal_latest, 2) if withdrawal_vals else None,
                "water_pressure_norm": round(water_pressure, 3),
                "n_rq_obs": len(rq_vals),
                "n_withdrawal_obs": len(withdrawal_vals),
            },
        }
