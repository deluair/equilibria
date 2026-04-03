"""Monetary Overhang: excess money supply above output-absorptive capacity.

Methodology
-----------
Monetary overhang is the accumulated excess of money supply beyond what is
warranted by real GDP and price level absorption (Portes & Santorum, 1987;
Holzmann et al., 1995).

    overhang = M2_growth - GDP_growth - inflation

Interpretation:
  - M2_growth = expansion of nominal money stock (% YoY)
  - GDP_growth = absorption by real output expansion (% YoY)
  - inflation = absorption through price level (% YoY)
  - Remainder = unabsorbed excess: inflationary pressure building

Persistent overhang (positive for multiple consecutive years) signals:
  - Suppressed inflation (price controls) with latent pressure
  - Coming inflation surge once controls lifted
  - Asset price inflation alternative channel

Scoring:
  rolling 3-year average overhang:
    score = clip(avg_overhang * 5, 0, 100)  if overhang > 0
    score = clip(-avg_overhang * 2, 0, 40)  if overhang < 0 (monetary tightness)

Sources: World Bank WDI
  FM.LBL.BMNY.GD.ZS  - Broad money (% of GDP) [YoY change as proxy for M2 growth]
  NY.GDP.MKTP.KD.ZG   - GDP growth (constant prices, %)
  FP.CPI.TOTL.ZG      - Inflation, consumer prices (annual %)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MonetaryOverhang(LayerBase):
    layer_id = "l15"
    name = "Monetary Overhang"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 20)

        series_map = {
            "broad_money_gdp": f"FM.LBL.BMNY.GD.ZS_{country}",
            "gdp_growth": f"NY.GDP.MKTP.KD.ZG_{country}",
            "inflation": f"FP.CPI.TOTL.ZG_{country}",
        }

        data: dict[str, dict[str, float]] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE series_id = ?) "
                "AND date >= date('now', ?) ORDER BY date",
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        if not data.get("broad_money_gdp"):
            return {"score": 50.0, "results": {"error": "insufficient broad money data"}}

        # Compute M2 growth (YoY % change in M2/GDP ratio as proxy)
        bm_dates = sorted(data["broad_money_gdp"])
        bm_vals = np.array([data["broad_money_gdp"][d] for d in bm_dates])
        if len(bm_vals) < 3:
            return {"score": 50.0, "results": {"error": "too few observations"}}

        m2_growth = np.diff(bm_vals) / np.maximum(np.abs(bm_vals[:-1]), 1e-6) * 100.0
        growth_dates = bm_dates[1:]

        # Compute overhang where all three series overlap
        overhang_series: list[float] = []
        overhang_dates: list[str] = []
        for i, d in enumerate(growth_dates):
            gdp_g = data.get("gdp_growth", {}).get(d)
            inf = data.get("inflation", {}).get(d)
            m2_g = float(m2_growth[i])
            if gdp_g is not None and inf is not None:
                oh = m2_g - float(gdp_g) - float(inf)
                overhang_series.append(oh)
                overhang_dates.append(d)

        if not overhang_series:
            # Fall back to M2 growth alone
            avg_growth = float(np.mean(m2_growth[-3:])) if len(m2_growth) >= 3 else float(np.mean(m2_growth))
            return {
                "score": round(float(np.clip(max(0.0, avg_growth) * 3.0, 0.0, 60.0)), 1),
                "results": {
                    "country": country,
                    "note": "no GDP/inflation data; using M2 growth only",
                    "m2_growth_mean_pct": round(avg_growth, 3),
                },
            }

        oh_arr = np.array(overhang_series)
        n = len(oh_arr)

        rolling_avg = float(np.mean(oh_arr[-3:])) if n >= 3 else float(np.mean(oh_arr))

        # Persistence: consecutive positive overhang years
        consecutive = 0
        max_consecutive = 0
        for v in oh_arr:
            if v > 0:
                consecutive += 1
                max_consecutive = max(max_consecutive, consecutive)
            else:
                consecutive = 0

        results: dict = {
            "country": country,
            "n_obs": n,
            "period": f"{overhang_dates[0]} to {overhang_dates[-1]}",
            "overhang_latest_pp": round(float(oh_arr[-1]), 3),
            "overhang_mean_pp": round(float(np.mean(oh_arr)), 3),
            "overhang_3yr_avg_pp": round(rolling_avg, 3),
            "max_consecutive_positive_yrs": max_consecutive,
            "persistent_overhang": max_consecutive >= 3,
            "pressure_building": rolling_avg > 5.0,
        }

        if rolling_avg > 0:
            score = float(np.clip(rolling_avg * 5.0, 0.0, 100.0))
        else:
            # Negative overhang = monetary tightness, mild stress signal
            score = float(np.clip(-rolling_avg * 2.0, 0.0, 40.0))

        if max_consecutive >= 5:
            score = min(score + 10.0, 100.0)

        return {"score": round(score, 1), "results": results}
