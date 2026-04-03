"""Composite financial stress index.

Combines three indicators into a normalized composite stress measure:
  - Domestic credit growth (FS.AST.DOMS.GD.ZS): credit boom = stress
  - Interest rate spread (FR.INR.LNDP): wide spread = intermediation stress
  - Exchange rate (PA.NUS.FCRF): FX volatility proxy via coefficient of variation

Each sub-component is min-max normalized to [0, 100] within the available
history, then averaged with equal weights. High composite = financial stress.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FinancialStressIndex(LayerBase):
    layer_id = "l7"
    name = "Financial Stress Index"

    _INDICATOR_KEYS = {
        "FS.AST.DOMS.GD.ZS": "credit",
        "FR.INR.LNDP": "spread",
        "PA.NUS.FCRF": "fx",
    }

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 15)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('FS.AST.DOMS.GD.ZS', 'FR.INR.LNDP', 'PA.NUS.FCRF')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.indicator_code, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no financial stress indicator data",
            }

        by_indicator: dict[str, list[float]] = {}
        for r in rows:
            by_indicator.setdefault(r["indicator_code"], []).append(float(r["value"]))

        components: dict[str, dict] = {}
        component_scores: list[float] = []

        # Credit component: high domestic credit growth = elevated stress
        credit_vals = by_indicator.get("FS.AST.DOMS.GD.ZS", [])
        if len(credit_vals) >= 2:
            arr = np.array(credit_vals)
            # Year-on-year change as credit boom indicator
            changes = np.diff(arr)
            boom_proxy = float(np.mean(np.abs(changes)))
            credit_score = float(np.clip(boom_proxy * 2.0, 0.0, 100.0))
            components["credit"] = {
                "indicator": "FS.AST.DOMS.GD.ZS",
                "latest_pct_gdp": round(float(arr[-1]), 2),
                "mean_abs_change": round(boom_proxy, 3),
                "component_score": round(credit_score, 2),
            }
            component_scores.append(credit_score)

        # Spread component: high spread = high intermediation stress
        spread_vals = by_indicator.get("FR.INR.LNDP", [])
        if spread_vals:
            spread_latest = float(np.mean(spread_vals[-3:]))
            spread_score = float(np.clip(spread_latest * 5.0, 0.0, 100.0))
            components["spread"] = {
                "indicator": "FR.INR.LNDP",
                "latest_pct": round(spread_latest, 3),
                "component_score": round(spread_score, 2),
            }
            component_scores.append(spread_score)

        # FX component: coefficient of variation of exchange rate = volatility
        fx_vals = by_indicator.get("PA.NUS.FCRF", [])
        if len(fx_vals) >= 3:
            arr = np.array(fx_vals)
            fx_mean = float(np.mean(arr))
            fx_std = float(np.std(arr, ddof=1))
            fx_cv = (fx_std / fx_mean * 100.0) if abs(fx_mean) > 1e-9 else 0.0
            fx_score = float(np.clip(fx_cv * 2.0, 0.0, 100.0))
            components["fx_volatility"] = {
                "indicator": "PA.NUS.FCRF",
                "coeff_variation_pct": round(fx_cv, 3),
                "component_score": round(fx_score, 2),
            }
            component_scores.append(fx_score)

        if not component_scores:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no components could be computed",
            }

        score = float(np.mean(component_scores))

        stress_level = (
            "severe" if score > 75
            else "elevated" if score > 50
            else "moderate" if score > 25
            else "low"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "components": components,
            "components_used": len(component_scores),
            "stress_level": stress_level,
        }
