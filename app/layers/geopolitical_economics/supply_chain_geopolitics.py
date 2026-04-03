"""Supply Chain Geopolitics: fuel + food critical import dependence.

Measures geopolitical risk embedded in supply chains by assessing dependence on
two categories of critical, non-substitutable imports: fuel (TM.VAL.FUEL.ZS.UN)
and food (TM.VAL.FOOD.ZS.UN) as shares of total merchandise imports. Countries
with high combined shares are exposed to supply chain weaponization.

Methodology:
    fuel_share = latest TM.VAL.FUEL.ZS.UN (fuel imports % merchandise imports)
    food_share = latest TM.VAL.FOOD.ZS.UN (food imports % merchandise imports)
    combined = fuel_share + food_share
    score = clip(combined * 0.8, 0, 100)

    At combined 0%: score = 0 (no critical supply chain dependence).
    At combined 125%: score = 100 (maximum, impossible in practice).
    Typical high-risk economy (fuel 30% + food 25% = 55%): score = 44.

Score (0-100): Higher = greater supply chain geopolitical risk.

References:
    Moran, T. (2021). "Supply Chain Vulnerabilities." PIIE Policy Brief.
    Keatinge, T. (2018). "Follow the Money." RUSI Occasional Paper.
    World Bank WDI TM.VAL.FUEL.ZS.UN, TM.VAL.FOOD.ZS.UN.
"""

from __future__ import annotations

from app.layers.base import LayerBase

_FUEL_CODE = "TM.VAL.FUEL.ZS.UN"
_FOOD_CODE = "TM.VAL.FOOD.ZS.UN"


class SupplyChainGeopolitics(LayerBase):
    layer_id = "lGP"
    name = "Supply Chain Geopolitics"

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
        fuel_share = await self._fetch_latest(db, _FUEL_CODE, "fuel imports merchandise")
        food_share = await self._fetch_latest(db, _FOOD_CODE, "food imports merchandise")

        if fuel_share is None and food_share is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for TM.VAL.FUEL.ZS.UN or TM.VAL.FOOD.ZS.UN"}

        combined = (fuel_share or 0.0) + (food_share or 0.0)
        score = float(min(max(combined * 0.8, 0.0), 100.0))

        return {
            "score": round(score, 2),
            "fuel_imports_pct_merchandise": round(fuel_share, 2) if fuel_share is not None else None,
            "food_imports_pct_merchandise": round(food_share, 2) if food_share is not None else None,
            "combined_critical_import_share": round(combined, 2),
            "metrics": {
                "fuel_indicator": _FUEL_CODE,
                "food_indicator": _FOOD_CODE,
            },
        }
