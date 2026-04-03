"""Strategic stockpile index: cereal production index as stockpile proxy.

Uses AG.PRD.CROP.XD (crop production index, base 2014-2016 = 100) combined with
reserve adequacy. Rising crop production relative to base signals improving
strategic food stockpile capacity.

Methodology:
    Fetch up to 15 observations of AG.PRD.CROP.XD. Use the most recent value.
    Normalize deviation from baseline:
        score = clip((110 - crop_index) * 2, 0, 100)

    crop_index = 110+: score = 0 (10%+ above baseline, strong stockpile capacity).
    crop_index = 100: score = 20 (at baseline, moderate capacity).
    crop_index = 60: score = 100 (40% below baseline, critical shortage).

Score (0-100): Higher score indicates weaker strategic stockpile adequacy.

References:
    World Bank WDI AG.PRD.CROP.XD.
    FAO (2022). "The State of Food Security and Nutrition in the World."
    OECD (2020). "Food Supply Chains and COVID-19: Impacts and Policy Lessons."
"""

from __future__ import annotations

from app.layers.base import LayerBase

_CODE = "AG.PRD.CROP.XD"
_NAME = "crop production index"


class StrategicStockpileIndex(LayerBase):
    layer_id = "lSR"
    name = "Strategic Stockpile Index"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CODE, f"%{_NAME}%"),
        )

        values = [float(r["value"]) for r in rows if r["value"] is not None]

        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"no data for {_CODE} (crop production index)",
            }

        crop_index = values[0]
        score = float(min(max((110.0 - crop_index) * 2.0, 0.0), 100.0))

        pct_above_baseline = round(crop_index - 100.0, 2)
        status = (
            "surplus" if crop_index >= 110
            else "adequate" if crop_index >= 100
            else "below_baseline" if crop_index >= 80
            else "critical"
        )

        return {
            "score": round(score, 2),
            "crop_production_index": round(crop_index, 2),
            "pct_vs_baseline_2014_2016": pct_above_baseline,
            "stockpile_status": status,
            "n_obs": len(values),
            "indicator": _CODE,
        }
