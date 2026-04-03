"""Fertilizer intensity: consumption kg/hectare and deviation from optimum.

Fertilizer intensity has a non-linear relationship with agricultural
performance and sustainability. Very low fertilizer use indicates a
productivity gap (underinvestment in soil nutrients), while very high use
signals environmental stress from nutrient runoff, soil acidification, and
water pollution.

Methodology:
    Fetch fertilizer consumption (kg per hectare of arable land) from
    WDI indicator AG.CON.FERT.ZS (fertilizer consumption kg/ha of arable land)
    or AG.CON.FERT.PT.ZS.

    The stress score is based on deviation from the empirical optimum of
    approximately 100 kg/ha, following a V-shaped penalty:

        if fertilizer_kg_ha < 100:
            score = clip((100 - fertilizer_kg_ha) / 100 * 60, 0, 60)  # underuse gap
        else:
            score = clip((fertilizer_kg_ha - 100) / 200 * 40, 0, 40)  # overuse stress

        Combined: score caps at 60 (severe underuse) or 40 (severe overuse).

    Benchmarks: <20 kg/ha = severe productivity gap; 80-120 kg/ha = optimal zone;
    >300 kg/ha = environmental stress threshold.

Score (0-100): Higher score indicates greater fertilizer-related stress,
from either underuse (productivity) or overuse (environmental).

References:
    World Bank WDI indicator AG.CON.FERT.ZS.
    Tilman, D. et al. (2002). "Agricultural sustainability and intensive
        production practices." Nature, 418, 671-677.
    Zhang, X. et al. (2015). "Managing nitrogen for sustainable development."
        Nature, 528, 51-59.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Optimal fertilizer use for scoring (kg/ha)
OPTIMUM_KG_HA = 100.0
# Threshold for environmental overuse stress
OVERUSE_THRESHOLD_KG_HA = 300.0
# Threshold for severe productivity gap
UNDERUSE_THRESHOLD_KG_HA = 20.0


class FertilizerIntensity(LayerBase):
    layer_id = "l5"
    name = "Fertilizer Intensity"

    async def compute(self, db, **kwargs) -> dict:
        """Compute fertilizer intensity and deviation-from-optimum score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
        """
        country = kwargs.get("country_iso3", "BGD")

        # Try indicator codes in order of preference
        row = None
        indicator_used = None
        for code in ("AG.CON.FERT.ZS", "AG.CON.FERT.PT.ZS"):
            row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.indicator_code = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, code),
            )
            if row and row["value"] is not None:
                indicator_used = code
                break

        if not row or row["value"] is None:
            row = await db.fetch_one(
                """
                SELECT dp.value, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE '%fertilizer%' AND ds.name LIKE '%kg%'
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )
            indicator_used = "name_match"

        if not row or row["value"] is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "fertilizer consumption data unavailable (AG.CON.FERT.ZS)",
            }

        fert_kg_ha = float(row["value"])
        latest_date = row["date"]

        # V-shaped score around optimum of 100 kg/ha
        if fert_kg_ha < OPTIMUM_KG_HA:
            # Underuse: productivity gap (max 60 points)
            score = float(np.clip((OPTIMUM_KG_HA - fert_kg_ha) / OPTIMUM_KG_HA * 60.0, 0.0, 60.0))
            stress_type = "underuse"
        else:
            # Overuse: environmental stress (max 40 points)
            score = float(np.clip((fert_kg_ha - OPTIMUM_KG_HA) / 200.0 * 40.0, 0.0, 40.0))
            stress_type = "overuse"

        # Historical trend
        history = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.indicator_code IN ('AG.CON.FERT.ZS', 'AG.CON.FERT.PT.ZS')
                   OR (ds.name LIKE '%fertilizer%' AND ds.name LIKE '%kg%'))
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

        intensity_category = (
            "critical_underuse" if fert_kg_ha < UNDERUSE_THRESHOLD_KG_HA
            else "underuse" if fert_kg_ha < OPTIMUM_KG_HA
            else "optimal" if fert_kg_ha <= 150
            else "above_optimal" if fert_kg_ha <= OVERUSE_THRESHOLD_KG_HA
            else "environmental_stress"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "fertilizer_kg_ha": round(fert_kg_ha, 2),
            "optimum_kg_ha": OPTIMUM_KG_HA,
            "overuse_threshold_kg_ha": OVERUSE_THRESHOLD_KG_HA,
            "underuse_threshold_kg_ha": UNDERUSE_THRESHOLD_KG_HA,
            "stress_type": stress_type,
            "intensity_category": intensity_category,
            "deviation_from_optimum_kg_ha": round(fert_kg_ha - OPTIMUM_KG_HA, 2),
            "latest_date": latest_date,
            "trend_slope_kg_ha_per_year": trend_slope,
            "indicator_used": indicator_used,
            "n_historical_obs": len(history),
        }
