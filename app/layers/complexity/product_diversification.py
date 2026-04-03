"""Product Diversification module.

Export product diversification via inverse Herfindahl-Hirschman Index (HHI)
computed from sectoral export shares (manufacturing, fuel, food).

High HHI = concentrated exports = structural vulnerability stress.

Sources: WDI TX.VAL.MANF.ZS.UN, TX.VAL.FUEL.ZS.UN, TX.VAL.FOOD.ZS.UN
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_SERIES = {
    "manufacturing": "TX.VAL.MANF.ZS.UN",
    "fuel": "TX.VAL.FUEL.ZS.UN",
    "food": "TX.VAL.FOOD.ZS.UN",
}


class ProductDiversification(LayerBase):
    layer_id = "lCP"
    name = "Product Diversification"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        shares: dict[str, float] = {}
        dates: dict[str, str] = {}

        for label, series_id in _SERIES.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, series_id),
            )
            if rows:
                shares[label] = float(rows[0]["value"])
                dates[label] = rows[0]["date"]

        if not shares:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        vals = np.array(list(shares.values()), dtype=float)
        # Normalize to shares summing to 1 (approximation from percentage points)
        total = vals.sum()
        if total <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero total shares"}

        norm_shares = vals / total
        hhi = float(np.sum(norm_shares ** 2))

        # HHI in [1/n, 1]. Score = HHI * 100 so max concentration = 100.
        score = min(100.0, max(0.0, hhi * 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "hhi": round(hhi, 4),
            "sector_shares_pct": {k: round(v, 2) for k, v in shares.items()},
            "sector_dates": dates,
            "n_sectors": len(shares),
            "interpretation": (
                "High HHI = concentrated exports = diversification stress. "
                "Low HHI = diversified export basket."
            ),
            "_citation": "World Bank WDI: TX.VAL.MANF.ZS.UN, TX.VAL.FUEL.ZS.UN, TX.VAL.FOOD.ZS.UN",
        }
