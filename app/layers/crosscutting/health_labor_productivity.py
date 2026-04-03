"""Health-Labor Productivity module.

Health expenditure vs labor productivity proxy (Bloom et al. 2004).

Queries current health expenditure as % of GDP (SH.XPD.CHEX.GD.ZS)
and GDP per capita growth (NY.GDP.PCAP.KD.ZG) as a labor productivity
proxy. Low health investment combined with stagnant per-capita growth
signals a health-productivity trap.

Score rises when health spend is low AND productivity growth is low.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import pearsonr

from app.layers.base import LayerBase

# Benchmarks from WHO and World Bank cross-country studies
_HEALTH_SPEND_THRESHOLD_LOW = 4.0   # <4% of GDP = low
_HEALTH_SPEND_THRESHOLD_HIGH = 7.0  # >7% of GDP = adequate
_GROWTH_THRESHOLD = 2.0             # <2% per capita growth = stagnant


class HealthLaborProductivity(LayerBase):
    layer_id = "lCX"
    name = "Health-Labor Productivity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_health = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.XPD.CHEX.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        rows_growth = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows_health or not rows_growth:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for health expenditure or per-capita growth",
            }

        health_map = {r["date"]: float(r["value"]) for r in rows_health if r["value"] is not None}
        growth_map = {r["date"]: float(r["value"]) for r in rows_growth if r["value"] is not None}

        common_dates = sorted(set(health_map) & set(growth_map))
        if len(common_dates) < 6:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"only {len(common_dates)} overlapping observations (need 6+)",
            }

        health_vals = np.array([health_map[d] for d in common_dates])
        growth_vals = np.array([growth_map[d] for d in common_dates])

        health_mean = float(np.mean(health_vals))
        growth_mean = float(np.mean(growth_vals))

        corr = 0.0
        p_value = 1.0
        if len(common_dates) >= 8:
            corr, p_value = pearsonr(health_vals, growth_vals)

        # Low health spend penalty (0-50 points)
        if health_mean < _HEALTH_SPEND_THRESHOLD_LOW:
            health_penalty = float(
                np.clip((_HEALTH_SPEND_THRESHOLD_LOW - health_mean) / _HEALTH_SPEND_THRESHOLD_LOW * 50.0, 0.0, 50.0)
            )
        elif health_mean < _HEALTH_SPEND_THRESHOLD_HIGH:
            health_penalty = float(
                (_HEALTH_SPEND_THRESHOLD_HIGH - health_mean) / (_HEALTH_SPEND_THRESHOLD_HIGH - _HEALTH_SPEND_THRESHOLD_LOW) * 20.0
            )
        else:
            health_penalty = 0.0

        # Low growth penalty (0-30 points)
        growth_penalty = float(np.clip((_GROWTH_THRESHOLD - growth_mean) / _GROWTH_THRESHOLD * 30.0, 0.0, 30.0))

        # Correlation bonus/penalty: positive corr reduces stress
        corr_adjustment = float(np.clip(-corr * 20.0, -20.0, 20.0))

        score = float(np.clip(health_penalty + growth_penalty + corr_adjustment, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "health_spend_mean_pct_gdp": round(health_mean, 2),
            "gdp_pcap_growth_mean": round(growth_mean, 2),
            "health_spend_productivity_corr": round(float(corr), 4),
            "p_value": round(float(p_value), 4),
            "health_penalty": round(health_penalty, 2),
            "growth_penalty": round(growth_penalty, 2),
            "interpretation": (
                "healthy investment-productivity link" if score < 25
                else "moderate health-productivity gap" if score < 50
                else "health-productivity trap"
            ),
            "reference": "Bloom, Canning & Sevilla 2004, AER 94(2)",
        }
