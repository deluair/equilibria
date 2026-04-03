"""Health expenditure share of GDP vs WHO benchmark.

Measures total health spending as a percentage of GDP and benchmarks against
the WHO-recommended threshold of 5% of GDP for basic health service coverage.
Uses World Bank WDI indicator SH.XPD.CHEX.GD.ZS (current health expenditure
as % of GDP) to identify countries with critically low health investment.

Key references:
    WHO (2010). Health systems financing: the path to universal coverage.
        World Health Report 2010.
    Xu, K. et al. (2011). Exploring the thresholds of health expenditure for
        protection against financial risk. World Health Report Background Paper 19.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthExpenditureShare(LayerBase):
    layer_id = "lHF"
    name = "Health Expenditure Share"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Compute health expenditure as % of GDP vs WHO 5% threshold.

        Fetches total current health expenditure as % of GDP across countries.
        Scores based on how far countries fall below the WHO recommended minimum.
        Higher scores indicate more countries below the threshold (stress).

        Returns dict with score, signal, and metrics on cross-country distribution.
        """
        rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.CHEX.GD.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No health expenditure share data in DB",
            }

        # Latest value per country
        latest: dict[str, float] = {}
        for row in rows:
            iso = row["country_iso3"]
            if iso not in latest and row["value"] is not None:
                latest[iso] = float(row["value"])

        values = list(latest.values())
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid health expenditure values",
            }

        who_threshold = 5.0  # WHO recommended minimum % of GDP
        below_threshold = [v for v in values if v < who_threshold]
        below_pct = 100.0 * len(below_threshold) / len(values)

        mean_spending = float(np.mean(values))
        median_spending = float(np.median(values))
        min_spending = float(np.min(values))
        max_spending = float(np.max(values))

        # Score: higher = more stress (more countries below threshold)
        # 0-25: <10% of countries below threshold
        # 25-50: 10-30% below
        # 50-75: 30-60% below
        # 75-100: >60% below
        score = float(np.clip(below_pct * (100.0 / 80.0), 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "n_countries": len(values),
                "mean_health_expenditure_pct_gdp": round(mean_spending, 2),
                "median_health_expenditure_pct_gdp": round(median_spending, 2),
                "min_pct_gdp": round(min_spending, 2),
                "max_pct_gdp": round(max_spending, 2),
                "who_threshold_pct_gdp": who_threshold,
                "countries_below_threshold": len(below_threshold),
                "pct_countries_below_threshold": round(below_pct, 1),
            },
        }
