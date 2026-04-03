"""Beveridge curve: vacancy-unemployment relationship.

The Beveridge curve plots the job vacancy rate against the unemployment rate.
It is downward-sloping: when unemployment is high, vacancies are low (slack),
and vice versa (tightness). Shifts of the curve indicate changes in matching
efficiency.

Specification:
    ln(v_t) = a + b*ln(u_t) + e_t

where v is the vacancy rate and u is the unemployment rate. The slope b
(Beveridge elasticity) is typically negative and around -1.

Outward shifts (higher vacancies for given unemployment) indicate:
    - Structural mismatch (skills, geography, sector)
    - Reduced search intensity
    - Increased hiring selectivity

The matching function (Petrongolo & Pissarides 2001):
    M(u, v) = A * u^alpha * v^(1-alpha)

where A is matching efficiency and alpha is the elasticity of matches wrt
unemployment (typically 0.5-0.7).

References:
    Beveridge, W. (1944). Full Employment in a Free Society.
    Blanchard, O. & Diamond, P. (1989). The Beveridge Curve. Brookings
        Papers on Economic Activity 1989(1): 1-76.
    Petrongolo, B. & Pissarides, C. (2001). Looking into the Black Box:
        A Survey of the Matching Function. Journal of Economic Literature
        39(2): 390-431.

Score: outward shift (high v AND high u) -> STRESS/CRISIS (mismatch).
Normal position on curve -> STABLE/WATCH.
"""

import numpy as np

from app.layers.base import LayerBase


class BeveridgeCurve(LayerBase):
    layer_id = "l3"
    name = "Beveridge Curve"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'beveridge'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient beveridge data"}

        import json

        dates = []
        u_rates = []
        v_rates = []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            u = meta.get("unemployment_rate")
            v = meta.get("vacancy_rate")
            if u is None or v is None:
                continue
            u, v = float(u), float(v)
            if u <= 0 or v <= 0:
                continue
            dates.append(row["date"])
            u_rates.append(u)
            v_rates.append(v)

        n = len(u_rates)
        if n < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        u = np.array(u_rates)
        v = np.array(v_rates)
        ln_u = np.log(u)
        ln_v = np.log(v)

        # Beveridge elasticity: ln(v) = a + b*ln(u)
        X = np.column_stack([np.ones(n), ln_u])
        beta = np.linalg.lstsq(X, ln_v, rcond=None)[0]
        resid = ln_v - X @ beta
        ss_res = np.sum(resid ** 2)
        ss_tot = np.sum((ln_v - ln_v.mean()) ** 2)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
        beveridge_elasticity = float(beta[1])

        # Detect shift: compare recent residuals to historical
        if n >= 20:
            split = n // 2
            early_resid_mean = float(np.mean(resid[:split]))
            late_resid_mean = float(np.mean(resid[split:]))
            shift_magnitude = late_resid_mean - early_resid_mean
        else:
            early_resid_mean = float(np.mean(resid[:n // 2]))
            late_resid_mean = float(np.mean(resid[n // 2:]))
            shift_magnitude = late_resid_mean - early_resid_mean

        # Current position
        latest_u = float(u[-1])
        latest_v = float(v[-1])
        tightness = latest_v / latest_u  # V/U ratio

        # Matching function estimation: ln(M) = ln(A) + alpha*ln(u) + (1-alpha)*ln(v)
        # Use hires proxy from metadata if available, otherwise skip

        # Score: based on mismatch signal
        # High u AND high v = outward shift = mismatch = STRESS
        # Normal inverse relationship = STABLE
        if shift_magnitude > 0.3:
            score = 55.0 + shift_magnitude * 50.0
        elif shift_magnitude > 0.1:
            score = 30.0 + (shift_magnitude - 0.1) * 125.0
        elif latest_u > 8:
            score = 40.0 + (latest_u - 8) * 5.0
        else:
            score = 10.0 + latest_u * 3.75
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "beveridge_elasticity": round(beveridge_elasticity, 4),
            "r_squared": round(r2, 4),
            "current": {
                "unemployment_rate": round(latest_u, 2),
                "vacancy_rate": round(latest_v, 2),
                "tightness_vu_ratio": round(tightness, 3),
            },
            "shift_analysis": {
                "early_period_residual_mean": round(early_resid_mean, 4),
                "late_period_residual_mean": round(late_resid_mean, 4),
                "shift_magnitude": round(shift_magnitude, 4),
                "outward_shift": shift_magnitude > 0.1,
                "interpretation": (
                    "structural mismatch increasing" if shift_magnitude > 0.1
                    else "matching efficiency stable"
                ),
            },
            "time_range": {
                "start": dates[0] if dates else None,
                "end": dates[-1] if dates else None,
            },
        }
