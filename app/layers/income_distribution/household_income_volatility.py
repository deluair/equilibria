"""Household Income Volatility module.

Measures household income instability using GDP per capita growth rate
volatility (NY.GDP.PCAP.KD.ZG) as the primary proxy. High variance in
growth rates signals chronic income insecurity at the household level.

Score = clip(growth_std * 5, 0, 100).

Sources: WDI (NY.GDP.PCAP.KD.ZG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HouseholdIncomeVolatility(LayerBase):
    layer_id = "lID"
    name = "Household Income Volatility"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
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

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        values = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        growth_mean = float(np.mean(values))
        growth_std = float(np.std(values, ddof=1))
        growth_min = float(np.min(values))
        growth_max = float(np.max(values))

        # Negative growth episodes: count recessions
        n_negative = int(np.sum(values < 0))
        negative_pct = n_negative / len(values) * 100

        # Coefficient of variation (abs scale, growth can be negative)
        cv = growth_std / max(abs(growth_mean), 1e-6) if growth_mean != 0 else growth_std

        # Recent volatility (last 5 years) vs full period
        recent = values[-5:] if len(values) >= 5 else values
        recent_std = float(np.std(recent, ddof=1)) if len(recent) > 1 else growth_std

        score = float(np.clip(growth_std * 5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(values),
            "period": f"{dates[0]} to {dates[-1]}",
            "growth_mean_pct": round(growth_mean, 3),
            "growth_std_pct": round(growth_std, 3),
            "growth_min_pct": round(growth_min, 3),
            "growth_max_pct": round(growth_max, 3),
            "recent_std_pct": round(recent_std, 3),
            "n_negative_growth_years": n_negative,
            "negative_growth_frequency_pct": round(negative_pct, 1),
            "coefficient_of_variation": round(cv, 4),
            "interpretation": "higher std = more volatile household income trajectory",
        }
