"""Gini coefficient of years of schooling distribution.

Computes the education Gini using the Barro-Lee attainment data disaggregated
by schooling level. The education Gini captures within-country inequality in
human capital accumulation, distinct from income inequality.

Formula (from Thomas et al. 2001):
    Gini_E = 1 - sum_i(p_i * (2 * mu_i_cumulative - s_i)) / mu

where p_i = population share at level i, s_i = years at level i,
mu = mean years of schooling.

References:
    Thomas, V., Wang, Y. & Fan, X. (2001). Measuring education inequality:
        Gini coefficients of education. World Bank Policy Research WP 2525.
    Checchi, D. (2006). The Economics of Education. Cambridge Univ. Press.

Score: high education Gini -> STRESS (inequality in human capital accumulation).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class EducationInequality(LayerBase):
    layer_id = "lED"
    name = "Education Inequality (Gini)"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Education Gini directly if stored
        gini_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'education_gini'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        gini = None
        latest_year = None

        gini_vals = [r["value"] for r in gini_rows if r["value"] is not None]
        if gini_vals:
            gini = gini_vals[0]
            latest_year = gini_rows[0]["date"][:4] if gini_rows[0]["date"] else None

        if gini is None:
            # Approximate from attainment distribution (no schooling, primary, secondary, tertiary)
            attain_rows = await db.fetch_all(
                """
                SELECT dp.value, ds.series_id, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id IN (
                      'SE.PRM.CUAT.ZS', 'SE.SEC.CUAT.LO.ZS',
                      'SE.SEC.CUAT.UP.ZS', 'SE.TER.CUAT.BA.ZS'
                  )
                ORDER BY dp.date DESC
                LIMIT 20
                """,
                (country,),
            )

            if not attain_rows:
                return {
                    "score": None,
                    "signal": "UNAVAILABLE",
                    "error": "no education distribution data",
                }

            # Group by series_id, take latest
            by_series: dict[str, float] = {}
            for r in attain_rows:
                sid = r["series_id"]
                if sid not in by_series and r["value"] is not None:
                    by_series[sid] = r["value"]

            # Years per level approximation
            levels = [
                ("no_school", 0.0, max(0, 100 - by_series.get("SE.PRM.CUAT.ZS", 0))),
                ("primary", 6.0, by_series.get("SE.PRM.CUAT.ZS", 0)),
                ("lower_sec", 9.0, by_series.get("SE.SEC.CUAT.LO.ZS", 0)),
                ("upper_sec", 12.0, by_series.get("SE.SEC.CUAT.UP.ZS", 0)),
                ("tertiary", 16.0, by_series.get("SE.TER.CUAT.BA.ZS", 0)),
            ]

            total_pct = sum(l[2] for l in levels)
            if total_pct <= 0:
                return {"score": None, "signal": "UNAVAILABLE", "error": "zero attainment shares"}

            shares = [(lbl, yrs, pct / total_pct) for lbl, yrs, pct in levels]
            mu = sum(yrs * sh for _, yrs, sh in shares)
            if mu <= 0:
                return {"score": None, "signal": "UNAVAILABLE", "error": "zero mean years schooling"}

            # Thomas et al. formula
            gini_num = 0.0
            cum_share = 0.0
            for _, yrs, sh in shares:
                cum_share += sh
                gini_num += sh * (2 * cum_share * mu - yrs * sh)
            gini = 1 - gini_num / mu

        gini = max(0.0, min(1.0, gini))

        # Score: Gini=0 -> perfect equality (stable), Gini=1 -> complete inequality (crisis)
        score = round(gini * 100.0, 2)

        return {
            "score": score,
            "country": country,
            "education_gini": round(gini, 4),
            "latest_year": latest_year,
            "interpretation": "0=perfect equality, 1=complete inequality in years of schooling",
        }
