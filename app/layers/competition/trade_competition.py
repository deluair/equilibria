"""Trade Competition module.

Measures import competitive pressure on domestic firms via import penetration
trends. Rising import shares compress domestic producer margins and force
incumbents to compete or exit.

Method:
- Query imports of goods and services as % of GDP (NE.IMP.GNFS.ZS) over time.
- Fit a linear trend to the import share series.
- Positive trend = rising import penetration = increasing competitive pressure
  on domestic firms (lower stress from market power perspective).
- Negative or stagnant trend = domestic incumbents protected from foreign rivals
  (higher market power stress).

Score interpretation (note: inverted):
  Falling imports (declining competition) -> high score.
  Rising imports (increasing competition) -> low score.
  score = clip(50 - trend_slope * 20, 0, 100).

Sources: WDI (NE.IMP.GNFS.ZS)
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class TradeCompetition(LayerBase):
    layer_id = "lCO"
    name = "Trade Competition"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.IMP.GNFS.ZS'
              AND dp.value IS NOT NULL
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient import data"}

        dates = [r["date"] for r in rows]
        values = []
        for r in rows:
            try:
                values.append(float(r["value"]))
            except (TypeError, ValueError):
                pass

        if len(values) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid observations"}

        vals = np.array(values)
        t = np.arange(len(vals), dtype=float)

        slope, intercept, r_value, p_value, std_err = sp_stats.linregress(t, vals)

        # Positive slope = rising import penetration = more competition = lower stress
        score = float(np.clip(50 - slope * 20, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "import_pct_gdp_latest": round(float(vals[-1]), 2),
            "import_pct_gdp_earliest": round(float(vals[0]), 2),
            "trend_slope_pp_per_year": round(float(slope), 4),
            "trend_r_squared": round(float(r_value**2), 4),
            "trend_p_value": round(float(p_value), 6),
            "n_obs": len(vals),
            "period": f"{dates[0]} to {dates[-1]}",
            "import_competition_rising": slope > 0,
            "interpretation": (
                "rising import competition (lower market power risk)" if slope > 0.5
                else "stable import exposure" if abs(slope) <= 0.5
                else "falling import competition (protected domestic market)"
            ),
            "reference": "Krugman (1979): import competition and domestic pricing",
        }
