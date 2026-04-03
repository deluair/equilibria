"""Critical input dependence: fuel + food imports as % of merchandise imports.

Uses TM.VAL.FUEL.ZS.UN (fuel imports as % of merchandise imports) and
TM.VAL.FOOD.ZS.UN (food imports as % of merchandise imports). Together they
measure dependence on two critical, non-substitutable input categories.

Methodology:
    Fetch most recent value for each indicator. Combined share:
        combined = fuel_share + food_share
        score = clip(combined * 0.7, 0, 100)

    At 0%: score = 0 (no critical dependence).
    At 143%: score = 100 (maximum critical input dependence, impossible in practice).
    Typical developing economy (fuel 20% + food 15% = 35%): score = 24.5.

    If only one indicator is available, score is computed on that alone.

Score (0-100): Higher score indicates greater critical input dependence.

References:
    World Bank WDI TM.VAL.FUEL.ZS.UN and TM.VAL.FOOD.ZS.UN.
    IMF (2022). "How Energy and Food Prices Affect Global Inflation."
    Borin & Mancini (2019). "Measuring What Matters in GVCs." World Bank.
"""

from __future__ import annotations

from app.layers.base import LayerBase

_FUEL_CODE = "TM.VAL.FUEL.ZS.UN"
_FOOD_CODE = "TM.VAL.FOOD.ZS.UN"


class CriticalInputDependence(LayerBase):
    layer_id = "lSR"
    name = "Critical Input Dependence"

    async def _fetch_latest(self, db, code: str, name: str) -> float | None:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        for r in rows:
            if r["value"] is not None:
                return float(r["value"])
        return None

    async def compute(self, db, **kwargs) -> dict:
        fuel = await self._fetch_latest(db, _FUEL_CODE, "fuel imports")
        food = await self._fetch_latest(db, _FOOD_CODE, "food imports")

        if fuel is None and food is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for TM.VAL.FUEL.ZS.UN or TM.VAL.FOOD.ZS.UN",
            }

        combined = (fuel or 0.0) + (food or 0.0)
        score = float(min(max(combined * 0.7, 0.0), 100.0))

        return {
            "score": round(score, 2),
            "fuel_imports_pct_merchandise": round(fuel, 2) if fuel is not None else None,
            "food_imports_pct_merchandise": round(food, 2) if food is not None else None,
            "combined_critical_share_pct": round(combined, 2),
            "fuel_indicator": _FUEL_CODE,
            "food_indicator": _FOOD_CODE,
        }
