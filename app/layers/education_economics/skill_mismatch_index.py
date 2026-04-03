"""Skills supply vs labor market demand gap.

Constructs a skill mismatch index as the normalized absolute difference between
education supply (share of tertiary graduates by field) and labor market demand
(employment share by occupational category requiring tertiary skills). Also
incorporates over-education and under-education rates where available.

References:
    McGuinness, S. (2006). Overeducation in the labour market.
        Journal of Economic Surveys, 20(3), 387-418.
    OECD (2017). Better Use of Skills in the Workplace. OECD Publishing.
    Leuven, E. & Oosterbeek, H. (2011). Overeducation and mismatch in the
        labour market. Handbook of the Economics of Education, 4, 283-326.

Score: high mismatch -> STRESS (inefficient human capital allocation).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SkillMismatchIndex(LayerBase):
    layer_id = "lED"
    name = "Skill Mismatch Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Direct skill mismatch index if pre-computed and stored
        mismatch_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'skill_mismatch'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        # Over-education rate (% workers with more education than job requires)
        over_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'over_education_rate'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        # Under-education rate
        under_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'under_education_rate'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        mismatch_idx = None
        over_rate = None
        under_rate = None

        m_vals = [r["value"] for r in mismatch_rows if r["value"] is not None]
        if m_vals:
            mismatch_idx = m_vals[0]

        o_vals = [r["value"] for r in over_rows if r["value"] is not None]
        if o_vals:
            over_rate = o_vals[0]

        u_vals = [r["value"] for r in under_rows if r["value"] is not None]
        if u_vals:
            under_rate = u_vals[0]

        if mismatch_idx is None and over_rate is None and under_rate is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no skill mismatch data",
            }

        # Derive composite if direct index unavailable
        if mismatch_idx is None:
            components = [v for v in [over_rate, under_rate] if v is not None]
            mismatch_idx = sum(components) / len(components)

        mismatch_idx = max(0.0, min(100.0, mismatch_idx))

        # Score maps directly: 0% mismatch = STABLE, 50%+ = CRISIS
        score = min(100.0, mismatch_idx * 2.0)

        return {
            "score": round(score, 2),
            "country": country,
            "skill_mismatch_index": round(mismatch_idx, 2),
            "over_education_rate_pct": round(over_rate, 2) if over_rate is not None else None,
            "under_education_rate_pct": round(under_rate, 2) if under_rate is not None else None,
            "interpretation": "% of workers with misaligned skills relative to job requirements",
        }
