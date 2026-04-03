"""Trade balance dynamics: exports minus imports as % GDP over time.

Tracks the trajectory of a country's external trade position. A persistent
and worsening current account on goods and services signals structural
vulnerabilities: import dependency, export competitiveness erosion, or
demand pressures that may require adjustment.

Metrics computed:
- Balance series: exports % GDP minus imports % GDP (WDI NE.EXP.GNFS.ZS
  and NE.IMP.GNFS.ZS)
- OLS trend: slope of balance over time
- Level: latest balance value (negative = deficit)
- Volatility: std dev of balance series
- Worsening deficit flag: trend negative AND latest balance negative

Score: higher score = more stress.
- Persistent deficit with worsening trend -> high score
- Surplus or improving trend -> low score
"""

import numpy as np

from app.layers.base import LayerBase


class TradeBalanceDynamics(LayerBase):
    layer_id = "l1"
    name = "Trade Balance Dynamics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.wdi_code
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.wdi_code IN ('NE.EXP.GNFS.ZS', 'NE.IMP.GNFS.ZS')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no exports/imports data"}

        exports: dict[str, float] = {}
        imports: dict[str, float] = {}

        for row in rows:
            val = row["value"]
            if val is None:
                continue
            code = row["wdi_code"]
            date = row["date"]
            if code == "NE.EXP.GNFS.ZS":
                exports[date] = float(val)
            elif code == "NE.IMP.GNFS.ZS":
                imports[date] = float(val)

        common_dates = sorted(set(exports.keys()) & set(imports.keys()))
        if len(common_dates) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient overlapping export/import data"}

        balance = np.array([exports[d] - imports[d] for d in common_dates])

        latest_balance = float(balance[-1])
        mean_balance = float(np.mean(balance))

        # OLS trend
        t = np.arange(len(balance), dtype=float)
        X = np.column_stack([np.ones(len(t)), t])
        beta = np.linalg.lstsq(X, balance, rcond=None)[0]
        trend_slope = float(beta[1])

        # Score construction
        # Deficit level penalty: deeper deficit = higher base score
        level_score = max(0.0, min(50.0, -latest_balance * 2.0)) if latest_balance < 0 else 0.0

        # Trend penalty: worsening (more negative slope) = higher stress
        trend_score = max(0.0, min(30.0, -trend_slope * 10.0)) if trend_slope < 0 else 0.0

        # Volatility penalty
        vol = float(np.std(balance))
        vol_score = min(20.0, vol * 1.5)

        score = float(np.clip(level_score + trend_score + vol_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "latest_balance_pct_gdp": round(latest_balance, 4),
            "mean_balance_pct_gdp": round(mean_balance, 4),
            "trend_slope_per_year": round(trend_slope, 6),
            "trend_direction": "improving" if trend_slope > 0 else "worsening",
            "balance_volatility": round(vol, 4),
            "worsening_deficit": bool(latest_balance < 0 and trend_slope < 0),
            "n_observations": len(common_dates),
            "date_range": [common_dates[0], common_dates[-1]],
        }
