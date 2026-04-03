"""Institutional Quality-Growth module.

Governance quality vs investment gap (Acemoglu, Johnson & Robinson 2001).

Queries World Governance Indicators: Rule of Law (RL.EST) or
Government Effectiveness (GE.EST) and Gross Capital Formation
(% GDP, NE.GDI.TOTL.ZS). Weak institutions depress investment
through property rights uncertainty, contract enforcement failures,
and political risk. Low governance + low investment = stress.

Score rises when governance estimates are low (negative WGI units)
AND investment rate is below the cross-country benchmark.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import pearsonr

from app.layers.base import LayerBase

# Cross-country benchmark: ~24% of GDP gross capital formation (World Bank median)
_INVESTMENT_BENCHMARK_PCT = 24.0
# WGI governance: 0 = world mean, negative = below average
_GOVERNANCE_STRESS_THRESHOLD = -0.5


class InstitutionalQualityGrowth(LayerBase):
    layer_id = "lCX"
    name = "Institutional Quality-Growth"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_gov = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND (ds.series_id LIKE 'RL.EST%' OR ds.series_id LIKE 'GE.EST%')
            ORDER BY dp.date
            """,
            (country,),
        )

        rows_inv = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.TOTL.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows_gov or not rows_inv:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for governance or investment",
            }

        gov_map = {r["date"]: float(r["value"]) for r in rows_gov if r["value"] is not None}
        inv_map = {r["date"]: float(r["value"]) for r in rows_inv if r["value"] is not None}

        common_dates = sorted(set(gov_map) & set(inv_map))
        if len(common_dates) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"only {len(common_dates)} overlapping observations (need 5+)",
            }

        gov_vals = np.array([gov_map[d] for d in common_dates])
        inv_vals = np.array([inv_map[d] for d in common_dates])

        gov_mean = float(np.mean(gov_vals))
        inv_mean = float(np.mean(inv_vals))

        corr = 0.0
        p_value = 1.0
        if len(common_dates) >= 8:
            corr, p_value = pearsonr(gov_vals, inv_vals)

        # Governance stress (0-50 points): below world mean governance
        if gov_mean < _GOVERNANCE_STRESS_THRESHOLD:
            gov_penalty = float(np.clip(
                abs(gov_mean - _GOVERNANCE_STRESS_THRESHOLD) / 1.5 * 50.0, 0.0, 50.0
            ))
        else:
            gov_penalty = 0.0

        # Investment gap (0-30 points)
        inv_gap = max(0.0, _INVESTMENT_BENCHMARK_PCT - inv_mean)
        inv_penalty = float(np.clip(inv_gap / _INVESTMENT_BENCHMARK_PCT * 30.0, 0.0, 30.0))

        # Positive corr between governance and investment -> reduces stress
        corr_adjustment = float(np.clip(-corr * 20.0, -20.0, 20.0))

        score = float(np.clip(gov_penalty + inv_penalty + corr_adjustment, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "governance_mean_wgi": round(gov_mean, 4),
            "investment_mean_pct_gdp": round(inv_mean, 2),
            "investment_benchmark_pct": _INVESTMENT_BENCHMARK_PCT,
            "governance_investment_corr": round(float(corr), 4),
            "p_value": round(float(p_value), 4),
            "governance_penalty": round(gov_penalty, 2),
            "investment_penalty": round(inv_penalty, 2),
            "interpretation": (
                "strong institutions-investment link" if score < 25
                else "moderate institutional gap" if score < 50
                else "institutional-investment trap"
            ),
            "reference": "Acemoglu, Johnson & Robinson 2001, AER 91(5)",
        }
