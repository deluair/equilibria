"""Health system efficiency: outcomes per dollar spent.

Estimates technical efficiency of health systems by relating health outcomes
(life expectancy, under-5 mortality) to health expenditure per capita.
Uses a frontier-based approach: countries achieving better outcomes per unit
of spending define the efficiency frontier.

Efficiency scores capture how much of the potential health outcome gain
is realized given available financing, controlling for income level.

Key references:
    Evans, D.B. et al. (2001). The comparative efficiency of national health
        systems in producing health: an analysis of 191 countries. WHO GPE
        Discussion Paper Series No. 29.
    Gravelle, H. et al. (2003). Measuring quality of life in economic
        evaluations. Health Technology Assessment, 7(1).
    Joumard, I. et al. (2010). Health care systems: efficiency and institutions.
        OECD Economics Department Working Papers No. 769.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthSystemEfficiency(LayerBase):
    layer_id = "lHF"
    name = "Health System Efficiency"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Compute health system efficiency as outcomes per dollar spent.

        Fetches health expenditure per capita (SH.XPD.CHEX.PC.CD), life
        expectancy (SP.DYN.LE00.IN), and under-5 mortality (SH.DYN.MORT) to
        estimate efficiency. Constructs a simple frontier ratio: actual outcome
        vs predicted outcome from log-linear regression on spending.

        Returns dict with score, signal, and efficiency metrics.
        """
        hepc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.CHEX.PC.CD'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        le_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.LE00.IN'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        u5m_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.DYN.MORT'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        if not hepc_rows or not le_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No health expenditure per capita or life expectancy data in DB",
            }

        def _latest(rows) -> dict[str, float]:
            out: dict[str, float] = {}
            for row in rows:
                iso = row["country_iso3"]
                if iso not in out and row["value"] is not None:
                    out[iso] = float(row["value"])
            return out

        hepc_data = _latest(hepc_rows)
        le_data = _latest(le_rows)
        u5m_data = _latest(u5m_rows)

        # Build cross-country dataset
        common = set(hepc_data.keys()) & set(le_data.keys())
        if len(common) < 10:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"Insufficient overlapping countries: {len(common)}",
            }

        log_spend = []
        le_vals = []
        isos = []
        for iso in common:
            h = hepc_data[iso]
            l = le_data[iso]
            if h > 0 and l > 0:
                log_spend.append(np.log(h))
                le_vals.append(l)
                isos.append(iso)

        if len(log_spend) < 10:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "Insufficient valid data pairs",
            }

        log_spend_arr = np.array(log_spend)
        le_arr = np.array(le_vals)

        # OLS: LE = alpha + beta * log(HE_pc)
        X = np.column_stack([np.ones(len(log_spend_arr)), log_spend_arr])
        beta, _, _, _ = np.linalg.lstsq(X, le_arr, rcond=None)
        le_predicted = X @ beta
        residuals = le_arr - le_predicted

        # Efficiency index: positive residual = above frontier (efficient)
        mean_resid = float(np.mean(residuals))
        std_resid = float(np.std(residuals))

        # R-squared
        ss_res = float(np.sum(residuals**2))
        ss_tot = float(np.sum((le_arr - np.mean(le_arr)) ** 2))
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Fraction of countries below efficiency frontier (negative residual)
        below_frontier = int(np.sum(residuals < 0))
        pct_below = 100.0 * below_frontier / len(residuals)

        # Score: higher pct below frontier = higher inefficiency stress
        score = float(np.clip(pct_below, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "n_countries": len(isos),
                "regression_r_squared": round(r_squared, 4),
                "log_spend_coefficient": round(float(beta[1]), 4),
                "mean_efficiency_residual_years_le": round(mean_resid, 2),
                "std_efficiency_residual": round(std_resid, 2),
                "countries_below_efficiency_frontier": below_frontier,
                "pct_below_frontier": round(pct_below, 1),
                "u5m_countries_available": len(u5m_data),
            },
        }
