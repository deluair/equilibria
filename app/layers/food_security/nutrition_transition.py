"""Nutrition transition failure: persistent stunting amid rising incomes.

The nutrition transition describes the shift in diet and disease patterns
accompanying economic development. A "transition failure" occurs when per
capita income rises rapidly but undernutrition outcomes (stunting) remain
elevated, indicating that growth is not translating into nutritional gains.
This signals structural failures in food systems, distribution, and care.

Methodology:
    stunting_pct : SH.STA.STNT.ZS (% of children under 5, most recent)
    gdp_growth   : NY.GDP.PCAP.KD.ZG (GDP per capita growth, annual %)
                   fetched as a time series to compute average recent growth.

    growth_avg = mean of last N observations of gdp_growth (N >= 3)

    stunting_stress = clip(stunting_pct * 2, 0, 100)

    transition_penalty: applies when growth > 3% but stunting > 20%
        transition_penalty = clip((growth_avg - 3) * 5, 0, 30) if stunting > 20

    score = clip(stunting_stress + transition_penalty, 0, 100)

Score (0-100): Higher score = greater nutrition transition failure.

References:
    Popkin, B.M. (1993). "Nutritional patterns and transitions."
        Population and Development Review, 19(1), 138-157.
    Victora, C.G. et al. (2010). "Maternal and child undernutrition:
        consequences for adult health and human capital." Lancet, 371, 340-357.
    World Bank (2023). WDI: SH.STA.STNT.ZS, NY.GDP.PCAP.KD.ZG.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class NutritionTransition(LayerBase):
    layer_id = "lFS"
    name = "Nutrition Transition"

    GROWTH_THRESHOLD = 3.0
    STUNTING_THRESHOLD = 20.0

    async def compute(self, db, **kwargs) -> dict:
        """Compute nutrition transition failure score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            growth_lookback : int - years of GDP growth to average (default 5)
        """
        country = kwargs.get("country_iso3", "BGD")
        growth_lookback = int(kwargs.get("growth_lookback", 5))

        stunting_row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'SH.STA.STNT.ZS'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )
        if not stunting_row:
            stunting_row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%stunting%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        growth_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY dp.date DESC
            LIMIT ?
            """,
            (country, growth_lookback),
        )
        if not growth_rows:
            growth_rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%GDP%per%capita%growth%'
                ORDER BY dp.date DESC
                LIMIT ?
                """,
                (country, growth_lookback),
            )

        if not stunting_row:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no stunting data available",
            }

        stunting_pct = float(stunting_row["value"]) if stunting_row["value"] is not None else None
        if stunting_pct is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "stunting value is null",
            }

        valid_growth = [float(r["value"]) for r in growth_rows if r["value"] is not None]
        growth_avg = float(np.mean(valid_growth)) if valid_growth else None

        stunting_stress = float(np.clip(stunting_pct * 2.0, 0, 100))

        transition_penalty = 0.0
        transition_failure = False
        if growth_avg is not None and growth_avg > self.GROWTH_THRESHOLD and stunting_pct > self.STUNTING_THRESHOLD:
            transition_penalty = float(np.clip((growth_avg - self.GROWTH_THRESHOLD) * 5.0, 0, 30))
            transition_failure = True

        score = float(np.clip(stunting_stress + transition_penalty, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "stunting_pct": round(stunting_pct, 2),
            "gdp_per_capita_growth_avg_pct": round(growth_avg, 2) if growth_avg is not None else None,
            "n_growth_obs": len(valid_growth),
            "component_scores": {
                "stunting_stress": round(stunting_stress, 2),
                "transition_penalty": round(transition_penalty, 2),
            },
            "transition_failure": transition_failure,
            "thresholds": {
                "growth_threshold_pct": self.GROWTH_THRESHOLD,
                "stunting_threshold_pct": self.STUNTING_THRESHOLD,
            },
            "stunting_date": stunting_row["date"],
            "indicators": ["SH.STA.STNT.ZS", "NY.GDP.PCAP.KD.ZG"],
        }
