"""Irrigation coverage: irrigated land as share of agricultural land.

Low irrigation coverage in water-scarce or drought-prone contexts represents
a significant climate vulnerability for agricultural systems, as rain-fed
agriculture is directly exposed to precipitation variability and droughts.

Methodology:
    Fetch irrigated land as % of agricultural land (WDI: AG.LND.IRIG.AG.ZS).
    The score captures under-irrigation as a climate vulnerability:

        score = clip(max(0, 30 - irrigation_pct) * 2.5, 0, 100)

    irrigation_pct = 0%: score = 75 (high vulnerability, very limited irrigation).
    irrigation_pct = 30%: score = 0 (well-irrigated, no stress by this measure).
    irrigation_pct > 30%: score = 0 (above threshold, no incremental stress).

    The threshold of 30% is motivated by the literature distinguishing
    predominantly rain-fed from irrigated agricultural systems.

Score (0-100): Higher score indicates greater climate vulnerability from
insufficient irrigation.

References:
    World Bank WDI indicator AG.LND.IRIG.AG.ZS.
    Siebert, S. et al. (2010). "Groundwater use for irrigation." Hydrology and
        Earth System Sciences, 14, 1863-1880.
    Rosegrant, M.W. & Perez, N.D. (1997). "Water Resources Development in
        Africa." IFPRI Discussion Paper.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class IrrigationCoverage(LayerBase):
    layer_id = "l5"
    name = "Irrigation Coverage"

    # Threshold above which irrigation coverage is considered adequate
    ADEQUATE_IRRIGATION_THRESHOLD = 30.0  # percent

    async def compute(self, db, **kwargs) -> dict:
        """Compute irrigation coverage and climate vulnerability score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
        """
        country = kwargs.get("country_iso3", "BGD")

        # Primary lookup by indicator code
        row = await db.fetch_one(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'AG.LND.IRIG.AG.ZS'
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        if not row:
            row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%irrig%' AND ds.name LIKE '%agricultural%land%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )

        if not row or row["value"] is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "irrigation coverage data unavailable (AG.LND.IRIG.AG.ZS)",
            }

        irrigation_pct = float(row["value"])
        latest_date = row["date"]

        # score = clip(max(0, 30 - irrigation_pct) * 2.5, 0, 100)
        shortfall = max(0.0, self.ADEQUATE_IRRIGATION_THRESHOLD - irrigation_pct)
        score = float(np.clip(shortfall * 2.5, 0.0, 100.0))

        # Historical trend
        history = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.indicator_code = 'AG.LND.IRIG.AG.ZS'
                   OR (ds.name LIKE '%irrig%' AND ds.name LIKE '%agricultural%land%'))
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        trend_slope = None
        if len(history) >= 5:
            from scipy.stats import linregress
            years = []
            vals = []
            for r in history:
                if r["value"] is not None:
                    try:
                        years.append(int(str(r["date"])[:4]))
                        vals.append(float(r["value"]))
                    except (ValueError, TypeError):
                        continue
            if len(years) >= 5:
                res = linregress(np.array(years, dtype=float), np.array(vals, dtype=float))
                trend_slope = round(float(res.slope), 4)

        coverage_level = (
            "well_irrigated" if irrigation_pct >= 30
            else "moderately_irrigated" if irrigation_pct >= 15
            else "poorly_irrigated" if irrigation_pct >= 5
            else "rain_fed"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "irrigation_pct": round(irrigation_pct, 2),
            "adequate_threshold_pct": self.ADEQUATE_IRRIGATION_THRESHOLD,
            "shortfall_pct": round(shortfall, 2),
            "coverage_level": coverage_level,
            "latest_date": latest_date,
            "trend_slope_pp_per_year": trend_slope,
            "indicator": "AG.LND.IRIG.AG.ZS",
            "n_historical_obs": len(history),
        }
