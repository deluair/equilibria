"""Beta convergence analysis for cross-country income dynamics.

Estimates unconditional and conditional beta convergence by regressing
per-capita GDP growth rates on initial income levels. A negative beta
coefficient indicates convergence (poorer countries growing faster).

Key references:
    Barro, R. & Sala-i-Martin, X. (1992). Convergence. JPE, 100(2), 223-251.
    Mankiw, N., Romer, D. & Weil, D. (1992). A contribution to the empirics
        of economic growth. QJE, 107(2), 407-437.
"""

from __future__ import annotations

import numpy as np
import statsmodels.api as sm

from app.layers.base import LayerBase


class BetaConvergence(LayerBase):
    layer_id = "l4"
    name = "Beta Convergence"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate unconditional and conditional beta convergence.

        Fetches cross-country GDP per capita data, computes average growth
        rates, and regresses them on log initial income. Optionally conditions
        on human capital, investment rate, and population growth.

        Returns dict with score, beta coefficient, convergence speed,
        half-life, and regression diagnostics.
        """
        country_iso3 = kwargs.get("country_iso3")

        rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not rows:
            return {"score": 50, "results": {"error": "no GDP per capita data"}}

        # Build panel: initial income and average growth per country
        countries: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            iso = r["country_iso3"]
            countries.setdefault(iso, []).append((r["date"], r["value"]))

        initial_log_gdp = []
        avg_growth = []
        isos = []
        for iso, series in countries.items():
            series.sort(key=lambda x: x[0])
            vals = [v for _, v in series if v and v > 0]
            if len(vals) < 5:
                continue
            y0 = vals[0]
            yt = vals[-1]
            t = len(vals) - 1
            if t <= 0 or y0 <= 0 or yt <= 0:
                continue
            g = (np.log(yt) - np.log(y0)) / t
            initial_log_gdp.append(np.log(y0))
            avg_growth.append(g)
            isos.append(iso)

        if len(initial_log_gdp) < 10:
            return {"score": 50, "results": {"error": "insufficient countries for convergence"}}

        y = np.array(avg_growth)
        x = np.array(initial_log_gdp)

        # Unconditional convergence regression
        X = sm.add_constant(x)
        model = sm.OLS(y, X)
        result = model.fit(cov_type="HC1")

        beta = float(result.params[1])
        se_beta = float(result.bse[1])
        p_beta = float(result.pvalues[1])
        r_sq = float(result.rsquared)
        n = int(result.nobs)

        # Convergence speed: beta = -(1 - e^{-lambda*T})/T approx -lambda for small T
        # For annualized: speed = -log(1 + beta*T)/T when T=1, speed approx -beta
        speed = -beta if beta < 0 else 0.0
        half_life = np.log(2) / speed if speed > 0 else float("inf")

        # Conditional convergence: add controls if available
        cond_results = None
        cond_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN (
                'SE.SEC.ENRR', 'NE.GDI.TOTL.ZS', 'SP.POP.GROW'
            )
            AND dp.date = (
                SELECT MAX(dp2.date) FROM data_points dp2
                WHERE dp2.series_id = ds.id
            )
            """
        )
        if cond_rows:
            controls: dict[str, dict[str, float]] = {}
            for r in cond_rows:
                controls.setdefault(r["country_iso3"], {})[r["series_id"]] = r["value"]

            cond_y, cond_x0, cond_controls = [], [], []
            for i, iso in enumerate(isos):
                if iso in controls and len(controls[iso]) >= 2:
                    c = controls[iso]
                    cond_y.append(avg_growth[i])
                    cond_x0.append(initial_log_gdp[i])
                    cond_controls.append([
                        c.get("SE.SEC.ENRR", 0),
                        c.get("NE.GDI.TOTL.ZS", 0),
                        c.get("SP.POP.GROW", 0),
                    ])

            if len(cond_y) >= 15:
                cond_X = np.column_stack([cond_x0, cond_controls])
                cond_X = sm.add_constant(cond_X)
                cond_model = sm.OLS(np.array(cond_y), cond_X)
                cond_result = cond_model.fit(cov_type="HC1")
                cond_beta = float(cond_result.params[1])
                cond_speed = -cond_beta if cond_beta < 0 else 0.0
                cond_half_life = np.log(2) / cond_speed if cond_speed > 0 else float("inf")
                cond_results = {
                    "beta": cond_beta,
                    "se": float(cond_result.bse[1]),
                    "pval": float(cond_result.pvalues[1]),
                    "r_sq": float(cond_result.rsquared),
                    "n_obs": int(cond_result.nobs),
                    "speed": cond_speed,
                    "half_life": cond_half_life,
                }

        # Score: strong convergence (negative beta, significant) = low score (stable)
        # No convergence or divergence = high score (stress)
        if beta < 0 and p_beta < 0.05:
            score = max(10, 40 - abs(beta) * 500)
        elif beta < 0:
            score = 45
        else:
            score = min(90, 55 + beta * 500)

        score = float(np.clip(score, 0, 100))

        results = {
            "unconditional": {
                "beta": beta,
                "se": se_beta,
                "pval": p_beta,
                "r_sq": r_sq,
                "n_obs": n,
                "speed": speed,
                "half_life": half_life,
            },
            "conditional": cond_results,
            "country_iso3": country_iso3,
            "n_countries": len(isos),
        }

        return {"score": score, "results": results}
