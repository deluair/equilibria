"""FX Intervention Frequency: frequency and size of FX interventions.

Methodology
-----------
Frequent or large FX interventions suggest the central bank cannot rely on
monetary policy alone to achieve external balance, often indicating:
  - Exchange rate misalignment
  - Imported inflation management
  - Capital flow volatility

Sarno & Taylor (2001): intervention effectiveness tied to signaling channel
and coordination with monetary policy. Frequent interventions outside
monetary policy context signal stress.

Proxies (direct intervention data rarely public):
  1. FX reserve volatility: high std(delta_reserves) -> frequent interventions
  2. Reserve change / GDP: size proxy
  3. FX reserve adequacy coverage: rapid depletion = defensive intervention

Score = clip(reserve_vol_pct * 10, 0, 100)
  Low reserve volatility -> low intervention frequency -> STABLE
  High volatility -> frequent intervention -> STRESS/CRISIS

Sources: WDI FI.RES.TOTL.CD (total FX reserves in USD),
         NY.GDP.MKTP.CD (GDP in USD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FxInterventionFrequency(LayerBase):
    layer_id = "lMY"
    name = "FX Intervention Frequency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 10)

        reserve_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'FI.RES.TOTL.CD'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.CD'
              AND dp.date >= date('now', ?)
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not reserve_rows or len(reserve_rows) < 4:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient reserve data"}

        reserves = np.array([float(r["value"]) for r in reserve_rows])
        dates = [r["date"] for r in reserve_rows]
        reserve_latest = float(reserves[-1])

        delta_reserves = np.diff(reserves)
        pct_changes = delta_reserves / (np.abs(reserves[:-1]) + 1e-12) * 100
        reserve_vol = float(np.std(pct_changes, ddof=1)) if len(pct_changes) > 1 else 0.0

        # Depletion episodes: two consecutive quarters of decline
        depletion_quarters = int(np.sum(delta_reserves < 0))
        depletion_rate = depletion_quarters / max(len(delta_reserves), 1)

        # Reserve / GDP ratio
        reserve_gdp_pct: float | None = None
        if gdp_rows:
            gdp_map = {r["date"]: float(r["value"]) for r in gdp_rows}
            res_map = {r["date"]: float(r["value"]) for r in reserve_rows}
            common = sorted(set(gdp_map) & set(res_map))
            if common:
                latest_common = common[-1]
                reserve_gdp_pct = res_map[latest_common] / gdp_map[latest_common] * 100

        score = float(np.clip(reserve_vol * 10.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "reserve_change_volatility_pct": round(reserve_vol, 2),
            "depletion_quarter_rate": round(depletion_rate, 3),
            "reserve_latest_usd_bn": round(reserve_latest / 1e9, 2),
            "reserve_gdp_pct": round(reserve_gdp_pct, 2) if reserve_gdp_pct is not None else None,
            "intervention_intensity": (
                "low" if reserve_vol < 5
                else "moderate" if reserve_vol < 10
                else "high"
            ),
            "n_obs": len(reserve_rows),
            "period": f"{dates[0]} to {dates[-1]}",
            "indicators": ["FI.RES.TOTL.CD", "NY.GDP.MKTP.CD"],
        }
