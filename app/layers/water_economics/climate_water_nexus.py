"""Climate-water nexus: climate disaster exposure compounding water stress.

Combines EN.CLC.MDAT.ZS (population share affected by climate-related disasters)
and ER.H2O.FWTL.ZS (freshwater withdrawal % of internal resources). Countries
facing frequent climate shocks with already high water stress have compounded
vulnerability to supply disruptions and infrastructure damage.

Sources: World Bank WDI (EN.CLC.MDAT.ZS, ER.H2O.FWTL.ZS)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ClimateWaterNexus(LayerBase):
    layer_id = "lWA"
    name = "Climate Water Nexus"

    async def compute(self, db, **kwargs) -> dict:
        climate_code = "EN.CLC.MDAT.ZS"
        climate_name = "affected by natural disasters"
        climate_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (climate_code, f"%{climate_name}%"),
        )

        withdrawal_code = "ER.H2O.FWTL.ZS"
        withdrawal_name = "freshwater withdrawals"
        withdrawal_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (withdrawal_code, f"%{withdrawal_name}%"),
        )

        climate_vals = [row["value"] for row in climate_rows if row["value"] is not None]
        withdrawal_vals = [row["value"] for row in withdrawal_rows if row["value"] is not None]

        if not climate_vals and not withdrawal_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No climate disaster or withdrawal data found",
            }

        climate_latest = float(climate_vals[0]) if climate_vals else None
        withdrawal_latest = float(withdrawal_vals[0]) if withdrawal_vals else None

        # Climate exposure: 0% = 0 risk, 10%+ = high
        climate_risk = min((climate_latest or 0.0) / 10.0, 1.0)
        # Water stress: 0% = 0, 100% = 1
        water_stress = min((withdrawal_latest or 25.0) / 100.0, 1.0)

        # Additive with interaction amplifier
        combined = climate_risk * 0.4 + water_stress * 0.4 + climate_risk * water_stress * 0.2
        score = round(min(100.0, combined * 100.0), 2)

        # Trend in climate frequency
        trend = "stable"
        if len(climate_vals) >= 3:
            recent = sum(float(v) for v in climate_vals[:3]) / 3
            older = sum(float(v) for v in climate_vals[-3:]) / 3
            if recent > older * 1.05:
                trend = "worsening"
            elif recent < older * 0.95:
                trend = "improving"

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "climate_affected_pct": round(climate_latest, 3) if climate_latest is not None else None,
                "withdrawal_pct": round(withdrawal_latest, 2) if withdrawal_latest is not None else None,
                "climate_risk_norm": round(climate_risk, 3),
                "water_stress_norm": round(water_stress, 3),
                "climate_trend": trend,
                "n_climate_obs": len(climate_vals),
                "n_withdrawal_obs": len(withdrawal_vals),
            },
        }
