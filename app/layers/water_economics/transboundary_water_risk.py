"""Transboundary water risk: political instability amplifying water stress.

Combines PV.EST (political stability and absence of violence, WGI) with
ER.H2O.FWTL.ZS (freshwater withdrawal %). Politically unstable countries
with high water stress face elevated transboundary conflict risk.

Sources: World Bank WDI/WGI (PV.EST, ER.H2O.FWTL.ZS)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class TransboundaryWaterRisk(LayerBase):
    layer_id = "lWA"
    name = "Transboundary Water Risk"

    async def compute(self, db, **kwargs) -> dict:
        pv_code = "PV.EST"
        pv_name = "political stability"
        pv_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pv_code, f"%{pv_name}%"),
        )

        withdrawal_code = "ER.H2O.FWTL.ZS"
        withdrawal_name = "freshwater withdrawals"
        withdrawal_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (withdrawal_code, f"%{withdrawal_name}%"),
        )

        pv_vals = [row["value"] for row in pv_rows if row["value"] is not None]
        withdrawal_vals = [row["value"] for row in withdrawal_rows if row["value"] is not None]

        if not pv_vals and not withdrawal_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No political stability or withdrawal data found",
            }

        # PV.EST ranges roughly -2.5 to +2.5; negative = more unstable
        pv_latest = float(pv_vals[0]) if pv_vals else 0.0
        # Normalize political risk: -2.5 -> 1.0, +2.5 -> 0.0
        political_risk = min(max((0.0 - pv_latest) / 2.5 * 0.5 + 0.5, 0.0), 1.0)

        withdrawal_latest = float(withdrawal_vals[0]) if withdrawal_vals else 25.0
        # Water stress component: 0% -> 0, 100%+ -> 1
        water_stress = min(withdrawal_latest / 100.0, 1.0)

        # Combined risk: multiplicative interaction (both needed to drive conflict)
        combined = (political_risk * 0.5 + water_stress * 0.3 + political_risk * water_stress * 0.2)
        score = round(min(100.0, combined * 100.0), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "political_stability_est": round(pv_latest, 3) if pv_vals else None,
                "political_risk_norm": round(political_risk, 3),
                "withdrawal_pct": round(withdrawal_latest, 2) if withdrawal_vals else None,
                "water_stress_norm": round(water_stress, 3),
                "n_pv_obs": len(pv_vals),
                "n_withdrawal_obs": len(withdrawal_vals),
            },
        }
