"""Total Factor Productivity (TFP) Estimation via Solow Residual.

TFP = GDP growth - capital_contribution - labor_contribution

Capital contribution = capital_share * investment_rate_change
Labor contribution  = labor_share * labor_force_growth

Low TFP residual indicates technological stagnation.

Sources: WDI (NY.GDP.MKTP.KD.ZG, NE.GDI.TOTL.ZS, SL.TLF.TOTL.IN)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_CAPITAL_SHARE = 0.35
_LABOR_SHARE = 0.65


class TFPEstimation(LayerBase):
    layer_id = "lTE"
    name = "TFP Estimation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )
        inv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'NE.GDI.TOTL.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )
        lab_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'SL.TLF.TOTL.IN'
            ORDER BY dp.date
            """,
            (country,),
        )

        if len(gdp_rows) < 5 or len(inv_rows) < 5 or len(lab_rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for TFP estimation",
            }

        gdp_vals = np.array([float(r["value"]) for r in gdp_rows])
        inv_vals = np.array([float(r["value"]) for r in inv_rows])
        lab_vals = np.array([float(r["value"]) for r in lab_rows])

        # Align lengths to shortest series
        n = min(len(gdp_vals), len(inv_vals), len(lab_vals))
        gdp_vals = gdp_vals[-n:]
        inv_vals = inv_vals[-n:]
        lab_vals = lab_vals[-n:]

        # Labor force growth rate
        lab_growth = np.diff(lab_vals) / (np.abs(lab_vals[:-1]) + 1e-10) * 100
        # Investment change as proxy for capital contribution driver
        inv_change = np.diff(inv_vals)

        n_obs = len(lab_growth)
        gdp_trimmed = gdp_vals[1:]  # align with diff series

        capital_contribution = _CAPITAL_SHARE * inv_change
        labor_contribution = _LABOR_SHARE * lab_growth
        tfp_residuals = gdp_trimmed - capital_contribution - labor_contribution

        mean_tfp = float(np.mean(tfp_residuals))
        std_tfp = float(np.std(tfp_residuals))

        # Low or negative TFP = technological stagnation = higher stress score
        # Benchmark: TFP around 1-2% is healthy for developed, 2-3% for emerging
        # Score: 0 = high TFP (no stress), 100 = deep negative TFP (severe stagnation)
        if mean_tfp >= 2.0:
            score = 0.0
        elif mean_tfp >= 0.0:
            score = (2.0 - mean_tfp) / 2.0 * 50.0
        else:
            # Negative TFP: increase stress sharply
            score = min(100.0, 50.0 + abs(mean_tfp) * 15.0)

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": n_obs,
            "mean_tfp_residual": round(mean_tfp, 4),
            "std_tfp_residual": round(std_tfp, 4),
            "capital_share_assumed": _CAPITAL_SHARE,
            "labor_share_assumed": _LABOR_SHARE,
            "period": f"{gdp_rows[1]['date']} to {gdp_rows[-1]['date']}",
            "method": "Solow residual: GDP growth - capital_contrib - labor_contrib",
        }
