"""Trade resilience: export diversification x reserve adequacy.

Measures a country's vulnerability to external trade shocks by combining two
dimensions:

1. Trade openness volatility (std dev of NE.TRD.GNFS.ZS over time) as a
   proxy for export diversification instability. Countries with highly
   concentrated export bases tend to exhibit greater openness volatility
   because a single commodity or partner dominates.

2. Import reserve coverage (FI.RES.TOTL.MO, months of import cover). The
   conventional adequacy threshold is 3 months; below it signals critical
   vulnerability.

Score formula (per specification):
    score = clip(100 - (openness_std / 10 + reserves_months * 3), 0, 100)

High score = high vulnerability (low diversification and/or low reserves).
Low score = resilient trade position.
"""

import numpy as np

from app.layers.base import LayerBase


class TradeResilience(LayerBase):
    layer_id = "l1"
    name = "Trade Resilience"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.wdi_code
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.wdi_code IN ('NE.TRD.GNFS.ZS', 'FI.RES.TOTL.MO')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no trade resilience data"}

        openness_series: list[float] = []
        reserves_series: list[float] = []

        for row in rows:
            val = row["value"]
            if val is None:
                continue
            code = row["wdi_code"]
            if code == "NE.TRD.GNFS.ZS":
                openness_series.append(float(val))
            elif code == "FI.RES.TOTL.MO":
                reserves_series.append(float(val))

        if not openness_series and not reserves_series:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for trade resilience indicators"}

        openness_std = float(np.std(openness_series)) if len(openness_series) >= 2 else 0.0
        reserves_months = float(np.mean(reserves_series[-5:])) if reserves_series else 3.0  # default neutral

        score = float(np.clip(100.0 - (openness_std / 10.0 + reserves_months * 3.0), 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "openness_std": round(openness_std, 4),
            "reserves_months_avg": round(reserves_months, 2),
            "n_openness_obs": len(openness_series),
            "n_reserves_obs": len(reserves_series),
            "interpretation": self._interpret(openness_std, reserves_months),
        }

    @staticmethod
    def _interpret(openness_std: float, reserves_months: float) -> str:
        parts = []
        if openness_std > 10.0:
            parts.append("high trade openness volatility")
        elif openness_std > 5.0:
            parts.append("moderate trade openness volatility")
        else:
            parts.append("stable trade openness")
        if reserves_months < 3.0:
            parts.append("critically low reserves")
        elif reserves_months < 6.0:
            parts.append("adequate reserves")
        else:
            parts.append("strong reserve cushion")
        return "; ".join(parts)
