"""Trade facilitation index: logistics and customs efficiency.

Trade facilitation encompasses the policies and infrastructure that allow
goods to cross borders efficiently. Poor logistics and high border costs
substantially reduce trade volumes (Anderson & Wincoop 2004, Limao &
Venables 2001).

Primary indicator: LP.LPI.OVRL.XQ (World Bank Logistics Performance
Index, overall score, 1-5 scale).
Fallback indicator: IC.EXP.COST.CD (cost to export, USD per container).

Score: low LPI or high cost-to-export = trade facilitation stress.
"""

import numpy as np

from app.layers.base import LayerBase


class TradeFacilitationIndex(LayerBase):
    layer_id = "l1"
    name = "Trade Facilitation Index"

    # LPI is surveyed every 2 years; look for either indicator
    _LPI_CODE = "LP.LPI.OVRL.XQ"
    _COST_CODE = "IC.EXP.COST.CD"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.wdi_code
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.wdi_code IN ('LP.LPI.OVRL.XQ', 'IC.EXP.COST.CD')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no trade facilitation data (LPI or export cost)"}

        lpi_vals: list[float] = []
        cost_vals: list[float] = []
        lpi_dates: list[str] = []
        cost_dates: list[str] = []

        for row in rows:
            val = row["value"]
            if val is None:
                continue
            code = row["wdi_code"]
            date = row["date"]
            if code == self._LPI_CODE:
                lpi_vals.append(float(val))
                lpi_dates.append(date)
            elif code == self._COST_CODE:
                cost_vals.append(float(val))
                cost_dates.append(date)

        # Prefer LPI; fall back to export cost
        if lpi_vals:
            arr = np.array(lpi_vals)
            latest = float(arr[-1])
            mean_val = float(np.mean(arr))
            indicator = "LPI (1-5)"
            dates = lpi_dates

            # LPI 1-5 scale; score inversely: LPI 1 = 100, LPI 5 = 0
            level_score = float(np.clip((5.0 - latest) / 4.0 * 80.0, 0.0, 80.0))

            # Trend: declining LPI adds penalty
            if len(arr) >= 2:
                t = np.arange(len(arr), dtype=float)
                X = np.column_stack([np.ones(len(t)), t])
                beta = np.linalg.lstsq(X, arr, rcond=None)[0]
                trend_slope = float(beta[1])
                trend_score = max(0.0, min(20.0, -trend_slope * 20.0)) if trend_slope < 0 else 0.0
            else:
                trend_slope = 0.0
                trend_score = 0.0

            score = float(np.clip(level_score + trend_score, 0.0, 100.0))
            result = {
                "score": round(score, 2),
                "country": country,
                "indicator": indicator,
                "latest_lpi": round(latest, 4),
                "mean_lpi": round(mean_val, 4),
                "trend_slope_per_survey": round(trend_slope, 6),
                "trend_direction": "improving" if trend_slope > 0 else "declining",
                "n_observations": len(lpi_vals),
                "date_range": [dates[0], dates[-1]],
            }
        elif cost_vals:
            arr = np.array(cost_vals)
            latest = float(arr[-1])
            mean_val = float(np.mean(arr))
            indicator = "Export cost (USD/container)"
            dates = cost_dates

            # Benchmark: $500 is relatively cheap; $3000+ is very expensive
            level_score = float(np.clip((latest - 500.0) / 2500.0 * 80.0, 0.0, 80.0))

            if len(arr) >= 2:
                t = np.arange(len(arr), dtype=float)
                X = np.column_stack([np.ones(len(t)), t])
                beta = np.linalg.lstsq(X, arr, rcond=None)[0]
                trend_slope = float(beta[1])
                trend_score = max(0.0, min(20.0, trend_slope / 100.0 * 5.0)) if trend_slope > 0 else 0.0
            else:
                trend_slope = 0.0
                trend_score = 0.0

            score = float(np.clip(level_score + trend_score, 0.0, 100.0))
            result = {
                "score": round(score, 2),
                "country": country,
                "indicator": indicator,
                "latest_export_cost_usd": round(latest, 2),
                "mean_export_cost_usd": round(mean_val, 2),
                "trend_slope_usd_per_year": round(trend_slope, 4),
                "trend_direction": "rising" if trend_slope > 0 else "falling",
                "n_observations": len(cost_vals),
                "date_range": [dates[0], dates[-1]],
            }
        else:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all trade facilitation data rows are null"}

        return result
