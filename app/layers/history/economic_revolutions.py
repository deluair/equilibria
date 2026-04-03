"""Economic Revolutions module.

Detects structural breaks in GDP growth via rolling standard deviation.
Periods of extreme volatility in the growth rate signal economic
disruptions analogous to revolutionary structural change -- whether
from liberalisation shocks, commodity booms, or institutional upheaval.

Indicator: NY.GDP.MKTP.KD.ZG (GDP growth, annual %, WDI).
Method: rolling std dev (window = 5 years). Maximum rolling std dev scores stress.
Score: clip(rolling_std_max * 10, 0, 100).
  - max rolling std = 0   -> 0   (perfectly stable)
  - max rolling std = 10  -> 100 (extreme volatility, e.g. war/hyperinflation)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_WINDOW = 5
_SCALE = 10.0  # reciprocal: std of 10 -> score 100


class EconomicRevolutions(LayerBase):
    layer_id = "lHI"
    name = "Economic Revolutions"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < _WINDOW + 1:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        growth = np.array([float(r["value"]) for r in rows])
        years = [r["date"][:4] for r in rows]

        # Rolling std dev
        rolling_std = np.array([
            float(np.std(growth[i: i + _WINDOW], ddof=1))
            for i in range(len(growth) - _WINDOW + 1)
        ])

        rolling_std_max = float(np.max(rolling_std))
        rolling_std_latest = float(rolling_std[-1])
        score = float(np.clip(rolling_std_max * _SCALE, 0, 100))

        # Identify peak volatility window
        peak_idx = int(np.argmax(rolling_std))
        peak_start = years[peak_idx]
        peak_end = years[min(peak_idx + _WINDOW - 1, len(years) - 1)]

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "period": f"{years[0]} to {years[-1]}",
            "rolling_window_years": _WINDOW,
            "rolling_std_max": round(rolling_std_max, 4),
            "rolling_std_latest": round(rolling_std_latest, 4),
            "peak_volatility_period": f"{peak_start} to {peak_end}",
            "latest_gdp_growth": round(float(growth[-1]), 3),
            "mean_gdp_growth": round(float(np.mean(growth)), 3),
        }
