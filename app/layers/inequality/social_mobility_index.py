"""Social Mobility Index module.

Composite measure of educational and economic mobility channels.
Blocked mobility occurs when children cannot surpass their parents' socioeconomic
status due to limited education access and stagnant economic opportunity.

Indicators:
- SE.TER.ENRR: School enrollment, tertiary (% gross) -- access to higher education
- SE.PRM.CMPT.ZS: Primary completion rate, total (% of relevant age group)
  -- foundation of educational ladder
- NY.GDP.PCAP.KD.ZG: GDP per capita growth (annual %) -- economic opportunity

Logic:
- Low tertiary enrollment: blocked upper mobility channel.
- Low primary completion: broken foundation.
- Low/negative GDP per capita growth: stagnant economy reduces all mobility.

Score:
    tertiary_penalty = clip((100 - tertiary_enrollment) / 100 * 40, 0, 40)
    primary_penalty  = clip((100 - primary_completion) / 100 * 30, 0, 30)
    growth_penalty   = clip((2 - gdp_pc_growth) * 3, 0, 30)
    score = clip(tertiary_penalty + primary_penalty + growth_penalty, 0, 100)

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SocialMobilityIndex(LayerBase):
    layer_id = "lIQ"
    name = "Social Mobility Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        tertiary_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.TER.ENRR'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        primary_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.PRM.CMPT.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD.ZG'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not tertiary_rows and not primary_rows and not growth_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        tertiary = float(tertiary_rows[0]["value"]) if tertiary_rows else 30.0
        primary_completion = float(primary_rows[0]["value"]) if primary_rows else 80.0
        gdp_pc_growth = float(growth_rows[0]["value"]) if growth_rows else 1.0
        has_tertiary = bool(tertiary_rows)
        has_primary = bool(primary_rows)
        has_growth = bool(growth_rows)

        # Cap tertiary at 100 (gross can exceed 100)
        tertiary_capped = float(np.clip(tertiary, 0, 100))

        tertiary_penalty = float(np.clip((100.0 - tertiary_capped) / 100.0 * 40.0, 0, 40))
        primary_penalty = float(np.clip((100.0 - primary_completion) / 100.0 * 30.0, 0, 30))
        growth_penalty = float(np.clip((2.0 - gdp_pc_growth) * 3.0, 0, 30))

        score = float(np.clip(tertiary_penalty + primary_penalty + growth_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "tertiary_enrollment_pct": round(tertiary, 2),
            "primary_completion_pct": round(primary_completion, 2),
            "gdp_pc_growth_pct": round(gdp_pc_growth, 3),
            "tertiary_source": "observed" if has_tertiary else "imputed_default",
            "primary_source": "observed" if has_primary else "imputed_default",
            "growth_source": "observed" if has_growth else "imputed_default",
            "tertiary_penalty": round(tertiary_penalty, 2),
            "primary_penalty": round(primary_penalty, 2),
            "growth_penalty": round(growth_penalty, 2),
            "interpretation": {
                "blocked_upper_mobility": tertiary_capped < 30,
                "broken_foundation": primary_completion < 70,
                "stagnant_economy": gdp_pc_growth < 1.0,
            },
        }
