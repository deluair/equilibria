"""Gender Pay Gap module.

Proxies the gender wage gap using labor force participation rate (LFPR)
differential between females and males, and tertiary education gender parity.

Where women participate less in the labor force and have lower educational
attainment relative to men, structural wage discrimination is more likely
to persist.

Indicators:
- SL.TLF.CACT.FE.ZS: Female labor force participation rate (% of female 15+)
- SL.TLF.CACT.MA.ZS: Male labor force participation rate (% of male 15+)
- SE.ENR.TERT.FM.ZS: School enrollment, tertiary, gender parity index (GPI)
  GPI = female/male enrollment ratio. GPI < 1 = male-dominated tertiary.

Score:
- LFPR gap component: (male_lfpr - female_lfpr) / 100 * 60, clipped 0-60.
- Education parity component: clip((1 - GPI) * 40, 0, 40) for GPI < 1;
  0 for GPI >= 1 (when women are equal or dominant in tertiary enrollment,
  pay gap is typically lower).

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GenderPayGap(LayerBase):
    layer_id = "lIQ"
    name = "Gender Pay Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        female_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.TLF.CACT.FE.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        male_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.TLF.CACT.MA.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        parity_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.ENR.TERT.FM.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not female_rows and not male_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        female_lfpr = float(female_rows[0]["value"]) if female_rows else 45.0
        male_lfpr = float(male_rows[0]["value"]) if male_rows else 70.0
        gpi = float(parity_rows[0]["value"]) if parity_rows else 1.0
        has_female = bool(female_rows)
        has_male = bool(male_rows)
        has_parity = bool(parity_rows)

        # LFPR gap: larger gap = more pay gap pressure
        lfpr_gap = max(0.0, male_lfpr - female_lfpr)
        lfpr_score = float(np.clip(lfpr_gap / 100.0 * 60.0, 0, 60))

        # Education parity: GPI < 1 adds score
        if gpi < 1.0:
            edu_score = float(np.clip((1.0 - gpi) * 40.0, 0, 40))
        else:
            edu_score = 0.0

        score = float(np.clip(lfpr_score + edu_score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "female_lfpr_pct": round(female_lfpr, 2),
            "male_lfpr_pct": round(male_lfpr, 2),
            "lfpr_gap_pct": round(lfpr_gap, 2),
            "tertiary_gender_parity_index": round(gpi, 4),
            "female_lfpr_source": "observed" if has_female else "imputed_default",
            "male_lfpr_source": "observed" if has_male else "imputed_default",
            "parity_source": "observed" if has_parity else "imputed_default",
            "lfpr_score": round(lfpr_score, 2),
            "education_parity_score": round(edu_score, 2),
            "interpretation": {
                "large_lfpr_gap": lfpr_gap > 20,
                "male_dominated_tertiary": gpi < 0.9,
            },
        }
