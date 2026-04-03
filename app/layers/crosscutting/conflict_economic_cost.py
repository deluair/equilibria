"""Conflict-Economic Cost module.

Military expenditure burden vs GDP growth (Collier & Hoeffler 2004).

Queries military expenditure as % of GDP (MS.MIL.XPND.GD.ZS) and
GDP growth (NY.GDP.MKTP.KD.ZG). High military burden crowds out
productive investment. The deviation of military spending from the
sample mean signals conflict-opportunity cost stress.

Score rises when military spending is high (above cross-country
median) AND GDP growth is low (crowding-out signal).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import pearsonr

from app.layers.base import LayerBase

# World Bank cross-country benchmark: ~2% of GDP military spending median
_MIL_SPEND_BENCHMARK = 2.0
_MIL_SPEND_HIGH = 4.0   # >4% = high burden
_GROWTH_LOW_THRESHOLD = 2.0


class ConflictEconomicCost(LayerBase):
    layer_id = "lCX"
    name = "Conflict-Economic Cost"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_mil = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'MS.MIL.XPND.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        rows_gdp = await db.fetch_all(
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

        if not rows_mil:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient military expenditure data",
            }

        mil_map = {r["date"]: float(r["value"]) for r in rows_mil if r["value"] is not None}
        gdp_map = {r["date"]: float(r["value"]) for r in rows_gdp if r["value"] is not None} if rows_gdp else {}

        mil_dates = sorted(mil_map)
        mil_vals = np.array([mil_map[d] for d in mil_dates])

        mil_mean = float(np.mean(mil_vals))
        mil_recent = float(mil_vals[-1]) if len(mil_vals) > 0 else mil_mean
        mil_trend = float(np.mean(np.diff(mil_vals))) if len(mil_vals) > 1 else 0.0

        # Military burden score (0-60 points)
        burden_excess = max(0.0, mil_mean - _MIL_SPEND_BENCHMARK)
        burden_score = float(np.clip(burden_excess / (_MIL_SPEND_HIGH - _MIL_SPEND_BENCHMARK) * 60.0, 0.0, 60.0))

        # Trend penalty: rising military spend -> additional stress
        trend_penalty = float(np.clip(mil_trend * 10.0, 0.0, 20.0))

        # Growth crowding-out penalty (if GDP data available)
        growth_penalty = 0.0
        corr = None
        p_value = None
        common_dates = sorted(set(mil_map) & set(gdp_map))
        growth_mean = None

        if len(common_dates) >= 6:
            common_mil = np.array([mil_map[d] for d in common_dates])
            common_gdp = np.array([gdp_map[d] for d in common_dates])
            growth_mean = float(np.mean(common_gdp))
            if len(common_dates) >= 8:
                corr_val, p_val = pearsonr(common_mil, common_gdp)
                corr = round(float(corr_val), 4)
                p_value = round(float(p_val), 4)
                # Negative corr (military crowds out growth) -> stress
                if corr_val < 0:
                    growth_penalty = float(np.clip(abs(corr_val) * 20.0, 0.0, 20.0))
            if growth_mean < _GROWTH_LOW_THRESHOLD and mil_mean > _MIL_SPEND_BENCHMARK:
                growth_penalty = min(20.0, growth_penalty + 10.0)

        score = float(np.clip(burden_score + trend_penalty + growth_penalty, 0.0, 100.0))

        result = {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(mil_vals),
            "period": f"{mil_dates[0]} to {mil_dates[-1]}" if mil_dates else "unknown",
            "military_spend_mean_pct_gdp": round(mil_mean, 2),
            "military_spend_recent": round(mil_recent, 2),
            "military_spend_trend": round(mil_trend, 4),
            "benchmark_pct_gdp": _MIL_SPEND_BENCHMARK,
            "burden_score": round(burden_score, 2),
            "trend_penalty": round(trend_penalty, 2),
            "growth_penalty": round(growth_penalty, 2),
            "interpretation": (
                "low conflict burden" if score < 25
                else "moderate military burden" if score < 50
                else "high conflict-economic cost"
            ),
            "reference": "Collier & Hoeffler 2004, Oxford Econ Papers 56(4)",
        }

        if growth_mean is not None:
            result["gdp_growth_mean"] = round(growth_mean, 2)
        if corr is not None:
            result["military_growth_corr"] = corr
            result["p_value"] = p_value

        return result
