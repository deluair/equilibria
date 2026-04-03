"""Barro-Lee human capital index.

Estimates the human capital stock using the Barro-Lee (2013) methodology:
average years of schooling by age group, weighted by Mincerian returns.
The human capital index H = exp(phi(S)) where phi is the piecewise linear
return function (Psacharopoulos 1994).

Penn World Table (PWT 10.0) also provides hc (human capital index) directly.

References:
    Barro, R. & Lee, J.W. (2013). A new data set of educational attainment
        in the world, 1950-2010. Journal of Development Economics, 104, 184-198.
    Psacharopoulos, G. (1994). Returns to investment in education: a global
        update. World Development, 22(9), 1325-1343.
    Feenstra, R., Inklaar, R. & Timmer, M. (2015). The next generation of the
        Penn World Tables. AER, 105(10), 3150-3182.

Score: low human capital index relative to income level -> STRESS.
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase


def _phi(s: float) -> float:
    """Piecewise Mincerian return function (Psacharopoulos 1994).

    Returns cumulative log-wage gain for s years of schooling:
        4% per year for first 4 years (primary)
        10% per year for years 5-8 (secondary lower)
        7% per year for years > 8 (post-secondary)
    """
    if s <= 4:
        return 0.13 * s
    elif s <= 8:
        return 0.13 * 4 + 0.10 * (s - 4)
    else:
        return 0.13 * 4 + 0.10 * 4 + 0.07 * (s - 8)


class HumanCapitalStock(LayerBase):
    layer_id = "lED"
    name = "Human Capital Stock (Barro-Lee)"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Average years of schooling, population 25+ (BAR-LEE or SE.SCH.LIFE.FE/MA.ZS)
        ays_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('BAR_LEE_MYS', 'HD.HCI.HLOS', 'SE.SCH.LIFE')
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        # Human capital index from PWT (direct)
        hc_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PWT_HC'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        hc_index = None
        hc_source = None
        mys = None

        # Prefer direct PWT index
        hc_vals = [r["value"] for r in hc_rows if r["value"] is not None]
        if hc_vals:
            hc_index = hc_vals[0]
            hc_source = "PWT"

        # Fallback: compute from mean years of schooling
        ays_vals = [r["value"] for r in ays_rows if r["value"] is not None]
        if ays_vals:
            mys = ays_vals[0]
            if hc_index is None:
                hc_index = math.exp(_phi(mys))
                hc_source = "Barro-Lee computed"

        if hc_index is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no human capital or schooling data",
            }

        # PWT hc ranges roughly 1.0 (0 years) to ~4.0 (16+ years, high-income)
        # Normalize: score is stress, so low hc = high stress
        # hc < 1.5 -> severe, 1.5-2.0 -> high stress, 2.0-2.5 -> watch, 2.5+ -> stable
        if hc_index < 1.5:
            score = 80.0
        elif hc_index < 2.0:
            score = 60.0
        elif hc_index < 2.5:
            score = 40.0
        elif hc_index < 3.0:
            score = 22.0
        else:
            score = 10.0

        return {
            "score": round(score, 2),
            "country": country,
            "human_capital_index": round(hc_index, 4),
            "mean_years_schooling": round(mys, 2) if mys is not None else None,
            "source": hc_source,
            "interpretation": "exp(phi(S)); 1.0=no schooling, higher=more human capital",
        }
