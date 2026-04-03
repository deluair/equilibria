"""Matching efficiency index: matching function efficiency from employment flows.

The matching function M(U, V) describes how unemployed workers (U) and vacancies
(V) combine to produce job matches. Matching efficiency (A) captures the
productivity of the search process independent of market tightness. A declining
efficiency indicates structural deterioration -- more search effort yields fewer
matches per unit time.

Proxy: employment-to-population ratio growth relative to unemployment rate changes.
Positive employment growth despite high unemployment signals efficient matching;
stagnant employment amid falling unemployment signals frictional withdrawal.

Score: high efficiency -> STABLE, moderate -> WATCH, low -> STRESS, failing -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MatchingEfficiencyIndex(LayerBase):
    layer_id = "lLM"
    name = "Matching Efficiency Index"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        emp_code = "SL.EMP.TOTL.SP.ZS"
        unemp_code = "SL.UEM.TOTL.ZS"

        emp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (emp_code, "%employment to population%"),
        )
        unemp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (unemp_code, "%unemployment%total%"),
        )

        emp_vals = [r["value"] for r in emp_rows if r["value"] is not None]
        unemp_vals = [r["value"] for r in unemp_rows if r["value"] is not None]

        if not emp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for employment-to-population ratio SL.EMP.TOTL.SP.ZS",
            }

        latest_emp = emp_vals[0]
        emp_trend = round(emp_vals[0] - emp_vals[-1], 3) if len(emp_vals) > 1 else None

        # Matching efficiency proxy: employment ratio level + trend direction
        # High employment ratio with positive trend = efficient matching
        base_score = max(0.0, 100.0 - latest_emp)  # lower emp ratio = higher stress

        # Adjust for trend: improving employment trend lowers stress
        if emp_trend is not None:
            if emp_trend > 2.0:
                base_score = max(0.0, base_score - 15.0)
            elif emp_trend > 0.5:
                base_score = max(0.0, base_score - 5.0)
            elif emp_trend < -2.0:
                base_score = min(100.0, base_score + 15.0)
            elif emp_trend < -0.5:
                base_score = min(100.0, base_score + 5.0)

        # Adjust for unemployment level: if both high unemp and low employment, worsen score
        if unemp_vals:
            unemp = unemp_vals[0]
            if unemp > 15:
                base_score = min(100.0, base_score + 10.0)
            elif unemp < 5:
                base_score = max(0.0, base_score - 5.0)

        score = round(base_score, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "employment_to_pop_ratio": round(latest_emp, 2),
                "employment_trend_pct": emp_trend,
                "unemployment_rate_pct": round(unemp_vals[0], 2) if unemp_vals else None,
                "n_obs_employment": len(emp_vals),
                "n_obs_unemp": len(unemp_vals),
                "matching_efficiency": "high" if score < 25 else "low" if score > 50 else "moderate",
            },
        }
