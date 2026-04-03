"""Migration-Development module.

Remittance dependency vs investment trap (Chami et al. 2003;
World Bank 2006 remittances and development).

Queries personal remittances received as % of GDP
(BX.TRF.PWKR.DT.GD.ZS) and Gross Capital Formation as % of GDP
(NE.GDI.TOTL.ZS). High remittance inflows that substitute for
rather than complement domestic investment signal a dependency trap:
consumption-driven growth without productive capital accumulation.

Score rises when remittances are high AND investment rate is low
(substitution effect). Low remittances with low investment is a
separate scarcity problem captured by other modules.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import pearsonr

from app.layers.base import LayerBase

# Benchmarks
_REMITTANCE_HIGH = 10.0    # >10% of GDP = high dependency
_REMITTANCE_MODERATE = 5.0
_INVESTMENT_LOW = 20.0     # <20% of GDP = low investment
_INVESTMENT_BENCHMARK = 24.0


class MigrationDevelopment(LayerBase):
    layer_id = "lCX"
    name = "Migration-Development"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_rem = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.TRF.PWKR.DT.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        rows_inv = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.TOTL.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows_rem:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient remittance data",
            }

        rem_map = {r["date"]: float(r["value"]) for r in rows_rem if r["value"] is not None}
        inv_map = {r["date"]: float(r["value"]) for r in rows_inv if r["value"] is not None} if rows_inv else {}

        rem_dates = sorted(rem_map)
        rem_vals = np.array([rem_map[d] for d in rem_dates])

        rem_mean = float(np.mean(rem_vals))
        rem_recent = float(rem_vals[-1]) if len(rem_vals) > 0 else rem_mean
        rem_trend = float(np.mean(np.diff(rem_vals))) if len(rem_vals) > 1 else 0.0

        # Remittance dependency stress (0-50 points)
        if rem_recent > _REMITTANCE_HIGH:
            rem_stress = float(
                np.clip(30.0 + (rem_recent - _REMITTANCE_HIGH) / _REMITTANCE_HIGH * 20.0, 30.0, 50.0)
            )
        elif rem_recent > _REMITTANCE_MODERATE:
            rem_stress = float(
                (rem_recent - _REMITTANCE_MODERATE) / (_REMITTANCE_HIGH - _REMITTANCE_MODERATE) * 30.0
            )
        else:
            rem_stress = 0.0

        # Investment gap stress and substitution effect
        inv_stress = 0.0
        substitution_penalty = 0.0
        corr = None
        p_value = None
        inv_mean = None

        common_dates = sorted(set(rem_map) & set(inv_map))
        if len(common_dates) >= 5:
            common_rem = np.array([rem_map[d] for d in common_dates])
            common_inv = np.array([inv_map[d] for d in common_dates])
            inv_mean = float(np.mean(common_inv))

            # Investment gap
            if inv_mean < _INVESTMENT_LOW:
                inv_stress = float(
                    np.clip((_INVESTMENT_LOW - inv_mean) / _INVESTMENT_LOW * 30.0, 0.0, 30.0)
                )
            elif inv_mean < _INVESTMENT_BENCHMARK:
                inv_stress = float(
                    (_INVESTMENT_BENCHMARK - inv_mean) / (_INVESTMENT_BENCHMARK - _INVESTMENT_LOW) * 15.0
                )

            # Substitution: negative corr between remittances and investment
            if len(common_dates) >= 8:
                corr_val, p_val = pearsonr(common_rem, common_inv)
                corr = round(float(corr_val), 4)
                p_value = round(float(p_val), 4)
                if corr_val < -0.2:  # Substitution effect
                    substitution_penalty = float(np.clip(abs(corr_val) * 20.0, 0.0, 20.0))

        score = float(np.clip(rem_stress + inv_stress + substitution_penalty, 0.0, 100.0))

        result = {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rem_vals),
            "period": f"{rem_dates[0]} to {rem_dates[-1]}" if rem_dates else "unknown",
            "remittances_recent_pct_gdp": round(rem_recent, 2),
            "remittances_mean_pct_gdp": round(rem_mean, 2),
            "remittances_trend": round(rem_trend, 4),
            "remittance_stress": round(rem_stress, 2),
            "investment_stress": round(inv_stress, 2),
            "substitution_penalty": round(substitution_penalty, 2),
            "interpretation": (
                "healthy migration-development link" if score < 25
                else "moderate remittance dependency" if score < 50
                else "remittance-investment dependency trap"
            ),
            "reference": "Chami et al. 2003 IMF WP/03/189; World Bank 2006",
        }

        if inv_mean is not None:
            result["investment_mean_pct_gdp"] = round(inv_mean, 2)
        if corr is not None:
            result["remittance_investment_corr"] = corr
            result["p_value"] = p_value

        return result
