"""Central Bank Independence: CBI proxy via inflation volatility relative to peers.

Methodology
-----------
Alesina & Summers (1993) established the empirical link between CBI and
inflation outcomes. Countries with independent central banks exhibit:
  - Lower mean inflation
  - Lower inflation variance (the key CBI proxy used here)

CBI proxy construction:
  std(inflation) -> low CBI countries have higher inflation volatility
  peer-adjusted: std(country) / median(peer_std)

Grilli, Masciandaro & Tabellini (1991) political-economic CBI index
components are approximated via:
  1. Inflation level (low level = price stability mandate respected)
  2. Inflation variance (low variance = credible commitment)
  3. Trend: improving = strengthening CBI culture

Score = clip(inflation_std * 5, 0, 100)
  std = 1%   -> score 5 (independent)
  std = 10%  -> score 50 (watch)
  std = 20%  -> score 100 (crisis)

Sources: World Bank WDI
  FP.CPI.TOTL.ZG  - Inflation, consumer prices (annual %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CentralBankIndependence(LayerBase):
    layer_id = "l15"
    name = "Central Bank Independence"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 20)

        rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE series_id = ?) "
            "AND date >= date('now', ?) ORDER BY date",
            (f"FP.CPI.TOTL.ZG_{country}", f"-{lookback} years"),
        )

        if not rows or len(rows) < 5:
            return {"score": 50.0, "results": {"error": "insufficient inflation data"}}

        dates = [r[0] for r in rows]
        inflation = np.array([float(r[1]) for r in rows])
        n = len(inflation)

        inf_mean = float(np.mean(inflation))
        inf_std = float(np.std(inflation, ddof=1))
        inf_latest = float(inflation[-1])

        # Rolling 5-year std to detect trend in volatility
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

        # Hyper-inflation episodes (>50%/yr) signal complete CBI breakdown
        hyperinflation_episodes = int(np.sum(inflation > 50.0))

        results: dict = {
            "country": country,
            "n_obs": n,
            "period": f"{dates[0]} to {dates[-1]}",
            "inflation_mean_pct": round(inf_mean, 3),
            "inflation_std_pct": round(inf_std, 3),
            "inflation_latest_pct": round(inf_latest, 3),
            "inflation_std_rolling_latest": round(rolling_std[-1], 3) if rolling_std else None,
            "std_trend_slope": round(std_trend_slope, 4) if std_trend_slope is not None else None,
            "volatility_increasing": std_trend_slope > 0 if std_trend_slope is not None else None,
            "hyperinflation_episodes": hyperinflation_episodes,
            "cbi_proxy_level": (
                "strong" if inf_std < 3.0
                else "moderate" if inf_std < 8.0
                else "weak"
            ),
        }

        # Score per spec: clip(inflation_std * 5, 0, 100)
        score = float(np.clip(inf_std * 5.0, 0.0, 100.0))
        if hyperinflation_episodes > 0:
            score = min(score + hyperinflation_episodes * 5.0, 100.0)

        return {"score": round(score, 1), "results": results}
