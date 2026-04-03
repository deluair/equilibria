"""Central Bank Independence: CBI index -- legal and actual independence.

Methodology
-----------
Alesina & Summers (1993): CBI inversely related to inflation mean and variance.
Cukierman (1992): both legal CBI (charter) and actual CBI (turnover rate) matter.

Proxy construction (in absence of direct CBI index data):
  1. Inflation stability proxy: low mean + low std => higher independence
     cbi_inflation = clip(1 - inf_std / 10, 0, 1)

  2. Inflation persistence: high AR(1) coefficient => lower independence
     (central bank cannot overcome inflation inertia)

  3. Trend: improving inflation control => strengthening de facto CBI

Score = clip(inf_std * 5 + (ar1 > 0.8)*20 + hyperinflation_episodes*5, 0, 100)
  Low score = high independence (STABLE)
  High score = low independence (STRESS/CRISIS)

Sources: WDI FP.CPI.TOTL.ZG
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CentralBankIndependence(LayerBase):
    layer_id = "lMY"
    name = "Central Bank Independence"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 20)

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FP.CPI.TOTL.ZG'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient inflation data"}

        inflation = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]
        n = len(inflation)

        inf_mean = float(np.mean(inflation))
        inf_std = float(np.std(inflation, ddof=1))
        inf_latest = float(inflation[-1])
        hyperinflation_episodes = int(np.sum(inflation > 50.0))

        ar1: float | None = None
        if n > 4:
            ar1 = float(np.corrcoef(inflation[:-1], inflation[1:])[0, 1])

        rolling_std: list[float] = []
        window = 5
        if n >= window:
            for i in range(window, n + 1):
                seg = inflation[i - window:i]
                rolling_std.append(float(np.std(seg, ddof=1)) if len(seg) > 1 else 0.0)

        std_trend_slope: float | None = None
        if len(rolling_std) >= 3:
            t = np.arange(len(rolling_std), dtype=float)
            std_trend_slope = float(np.polyfit(t, rolling_std, 1)[0])

        score = float(np.clip(inf_std * 5.0, 0.0, 100.0))
        if ar1 is not None and ar1 > 0.8:
            score = min(score + 20.0, 100.0)
        score = min(score + hyperinflation_episodes * 5.0, 100.0)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": n,
            "period": f"{dates[0]} to {dates[-1]}",
            "inflation_mean_pct": round(inf_mean, 3),
            "inflation_std_pct": round(inf_std, 3),
            "inflation_latest_pct": round(inf_latest, 3),
            "ar1_persistence": round(ar1, 3) if ar1 is not None else None,
            "hyperinflation_episodes": hyperinflation_episodes,
            "std_trend_slope": round(std_trend_slope, 4) if std_trend_slope is not None else None,
            "volatility_increasing": std_trend_slope > 0 if std_trend_slope is not None else None,
            "cbi_proxy_level": (
                "strong" if inf_std < 3.0
                else "moderate" if inf_std < 8.0
                else "weak"
            ),
        }
