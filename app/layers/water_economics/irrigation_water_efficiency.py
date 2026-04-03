"""Irrigation water efficiency: irrigated land relative to cereal yield performance.

Combines AG.LND.IRIG.AG.ZS (irrigated land as % of agricultural land) and
AG.YLD.CREL.KG (cereal yield, kg per hectare). High irrigated land share with
low cereal yields signals poor water-to-output conversion in agriculture.

Sources: World Bank WDI (AG.LND.IRIG.AG.ZS, AG.YLD.CREL.KG)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class IrrigationWaterEfficiency(LayerBase):
    layer_id = "lWA"
    name = "Irrigation Water Efficiency"

    async def compute(self, db, **kwargs) -> dict:
        irrig_code = "AG.LND.IRIG.AG.ZS"
        irrig_name = "irrigated land"
        irrig_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (irrig_code, f"%{irrig_name}%"),
        )

        yield_code = "AG.YLD.CREL.KG"
        yield_name = "cereal yield"
        yield_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (yield_code, f"%{yield_name}%"),
        )

        irrig_vals = [row["value"] for row in irrig_rows if row["value"] is not None]
        yield_vals = [row["value"] for row in yield_rows if row["value"] is not None]

        if not irrig_vals and not yield_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No irrigation or cereal yield data found",
            }

        irrig_latest = float(irrig_vals[0]) if irrig_vals else None
        yield_latest = float(yield_vals[0]) if yield_vals else None

        # Efficiency penalty: more irrigated land without proportionally higher yield
        # Normalize irrigation: 0-100 % of ag land -> 0-1
        irrig_norm = (irrig_latest / 100.0) if irrig_latest is not None else 0.5
        # Normalize yield: global average ~3500 kg/ha; >7000 = high efficiency
        yield_norm = min(yield_latest / 7000.0, 1.0) if yield_latest is not None else 0.3

        # Higher irrigation share with lower yield = inefficiency = higher risk
        inefficiency = irrig_norm * (1.0 - yield_norm)
        score = round(min(100.0, inefficiency * 100.0), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "irrigated_land_pct": round(irrig_latest, 2) if irrig_latest is not None else None,
                "cereal_yield_kg_ha": round(yield_latest, 1) if yield_latest is not None else None,
                "irrigation_norm": round(irrig_norm, 3),
                "yield_norm": round(yield_norm, 3),
                "inefficiency_index": round(inefficiency, 3),
                "n_irrig_obs": len(irrig_vals),
                "n_yield_obs": len(yield_vals),
            },
        }
