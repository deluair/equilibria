"""University enrollment growth vs economic absorption capacity.

Measures the rate of tertiary enrollment expansion and contrasts it with
the economy's demonstrated ability to absorb graduates into skilled employment.
Rapid expansion without commensurate labor demand leads to over-education,
graduate unemployment, and wage compression for degree holders.

Indicator: tertiary enrollment CAGR (5-year) minus graduate employment
absorption rate. Positive gap -> oversupply pressure.

References:
    Freeman, R.B. (1976). The Overeducated American. Academic Press.
    Carnoy, M. (1995). Rates of Return to Education. IIEP-UNESCO.
    World Bank (2019). Missed Opportunities: The High Cost of Not Educating
        Girls. World Bank.

Score: high expansion with low absorption -> STRESS (graduate unemployment risk).
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase


class TertiaryExpansionRate(LayerBase):
    layer_id = "lED"
    name = "Tertiary Expansion Rate"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Gross tertiary enrollment ratio over time (SE.TER.ENRR)
        enroll_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SE.TER.ENRR'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        # Graduate unemployment rate or youth unemployment with tertiary education
        grad_unemp_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'graduate_unemployment'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        enroll_vals = [(r["date"][:4], r["value"]) for r in enroll_rows if r["value"] is not None and r["date"]]
        if len(enroll_vals) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient tertiary enrollment time series",
            }

        # CAGR over available window
        enroll_vals = sorted(enroll_vals, key=lambda x: x[0])
        t_start, v_start = enroll_vals[0]
        t_end, v_end = enroll_vals[-1]

        years_span = int(t_end) - int(t_start)
        if years_span <= 0 or v_start <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "invalid enrollment data"}

        cagr = (math.pow(v_end / v_start, 1.0 / years_span) - 1) * 100.0

        grad_unemp = None
        u_vals = [r["value"] for r in grad_unemp_rows if r["value"] is not None]
        if u_vals:
            grad_unemp = u_vals[0]

        # Score: rapid expansion (cagr > 5%) with high grad unemployment -> STRESS
        base_score = min(60.0, max(0.0, (cagr - 2.0) * 8.0))
        if grad_unemp is not None:
            unemp_penalty = min(40.0, grad_unemp * 1.5)
            score = base_score + unemp_penalty
        else:
            score = base_score + 20.0  # uncertainty penalty when data absent

        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "tertiary_enrollment_latest_pct": round(v_end, 2),
            "tertiary_enrollment_earliest_pct": round(v_start, 2),
            "years_span": years_span,
            "cagr_pct": round(cagr, 3),
            "graduate_unemployment_pct": round(grad_unemp, 2) if grad_unemp is not None else None,
            "interpretation": "CAGR of tertiary enrollment; high CAGR + grad unemployment = oversupply",
        }
