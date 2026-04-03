"""Inventory buffer adequacy: import cover as buffer proxy.

Uses FI.RES.TOTL.MO (total reserves in months of imports). This measure
captures the buffer stock available to absorb supply chain disruptions. More
months of import cover signals greater resilience.

Methodology:
    Fetch up to 15 observations of FI.RES.TOTL.MO. Use the most recent value.
    Adequate buffer is defined as >= 6 months (IMF minimum recommendation is 3 months).
    score = clip((6 - reserves_months) / 6 * 100, 0, 100).

    At 6+ months: score = 0 (adequate buffer).
    At 0 months: score = 100 (no buffer, maximum vulnerability).
    At 3 months (IMF minimum): score = 50.

Score (0-100): Higher score indicates lower inventory buffer adequacy.

References:
    World Bank WDI FI.RES.TOTL.MO.
    IMF (2016). "Guidance Note on the Assessment of Reserve Adequacy."
    Aizenman & Lee (2007). "International reserves: precautionary versus mercantilist views."
"""

from __future__ import annotations

from app.layers.base import LayerBase

_CODE = "FI.RES.TOTL.MO"
_NAME = "reserves in months of imports"


class InventoryBufferAdequacy(LayerBase):
    layer_id = "lSR"
    name = "Inventory Buffer Adequacy"

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
                "error": f"no data for {_CODE} (import cover months)",
            }

        reserves_months = values[0]
        score = float(min(max((6.0 - reserves_months) / 6.0 * 100.0, 0.0), 100.0))

        adequacy = (
            "strong" if reserves_months >= 6.0
            else "adequate" if reserves_months >= 3.0
            else "low" if reserves_months >= 1.5
            else "critical"
        )

        return {
            "score": round(score, 2),
            "import_cover_months": round(reserves_months, 2),
            "buffer_adequacy": adequacy,
            "imf_minimum_threshold_months": 3.0,
            "n_obs": len(values),
            "indicator": _CODE,
        }
