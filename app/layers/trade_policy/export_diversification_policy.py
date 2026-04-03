"""Export Diversification Policy module.

Assesses export portfolio concentration using a Herfindahl-Hirschman Index (HHI)
computed from manufacturing, services, and food/agriculture export shares.
High concentration indicates weak diversification policy.

Score = clip(HHI * 100, 0, 100)
where HHI = sum of squared shares (shares as decimals)

Sources: WDI
  TX.VAL.MANF.ZS.UN - Manufactures exports (% of merchandise exports)
  TX.VAL.SERV.ZS.WT - Commercial service exports (% of total trade)
  TX.VAL.FOOD.ZS.UN - Food exports (% of merchandise exports)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ExportDiversificationPolicy(LayerBase):
    layer_id = "lTP"
    name = "Export Diversification Policy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        series_map = {
            "manufacturing": "TX.VAL.MANF.ZS.UN",
            "services": "TX.VAL.SERV.ZS.WT",
            "food": "TX.VAL.FOOD.ZS.UN",
        }

        shares = {}
        for label, sid in series_map.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                """,
                (country, sid),
            )
            vals = [float(r["value"]) for r in rows if r["value"] is not None]
            if vals:
                shares[label] = float(np.mean(vals[:5]))

        if len(shares) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient export composition data",
            }

        share_values = list(shares.values())
        total = sum(share_values)

        if total <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero total export shares"}

        # Normalise to fractions (some series may not sum to 100 due to different bases)
        fractions = [s / 100.0 for s in share_values]

        # HHI on available sectors; missing sectors inflate concentration implicitly
        hhi = float(sum(f**2 for f in fractions))
        score = float(np.clip(hhi * 100, 0, 100))

        diversification_level = (
            "well diversified" if hhi < 0.15
            else "moderately diversified" if hhi < 0.30
            else "concentrated" if hhi < 0.50
            else "highly concentrated"
        )

        return {
            "score": round(score, 1),
            "country": country,
            "hhi": round(hhi, 4),
            "sector_shares_pct": {k: round(v, 2) for k, v in shares.items()},
            "diversification_level": diversification_level,
            "n_sectors": len(shares),
        }
