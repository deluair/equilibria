"""Cost-effectiveness frontier: health outcomes per unit of health spending.

Estimates the efficiency of health technology investment by computing a
life-expectancy-to-expenditure ratio: life expectancy at birth (SP.DYN.LE00.IN)
divided by current health expenditure as % of GDP (SH.XPD.CHEX.GD.ZS). Countries
achieving high life expectancy relative to their health spending are on the
cost-effectiveness frontier; those achieving poor outcomes per dollar spent are
furthest from it (highest stress).

Key references:
    Papanicolas, I. et al. (2018). Health care spending in the United States
        and other high-income countries. JAMA, 319(10), 1024-1039.
    Gallet, C.A. & Doucouliagos, H. (2017). The impact of transfers on economic
        growth: a meta-analysis. Economic Modelling, 64, 270-283.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CostEffectivenessFrontier(LayerBase):
    layer_id = "lHT"
    name = "Cost Effectiveness Frontier"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score cost-effectiveness as life expectancy per health spending unit.

        Computes life expectancy / health expenditure ratio. High ratio means
        more life-years per health dollar (low stress). Low ratio (poor outcomes
        per dollar) signals inefficient health technology use (high stress).

        Returns dict with score, signal, and efficiency metrics.
        """
        le_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SP.DYN.LE00.IN", "%life expectancy%birth%"),
        )
        chex_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SH.XPD.CHEX.GD.ZS", "%current health expenditure%"),
        )

        le_values = [float(r["value"]) for r in le_rows if r["value"] is not None]
        chex_values = [float(r["value"]) for r in chex_rows if r["value"] is not None]

        if not le_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No life expectancy data in DB",
            }

        if not chex_values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No health expenditure data in DB",
            }

        median_le = float(np.median(le_values))
        median_chex = float(np.median(chex_values))

        metrics: dict = {
            "mean_life_expectancy_years": round(float(np.mean(le_values)), 2),
            "median_life_expectancy_years": round(median_le, 2),
            "n_le_obs": len(le_values),
            "mean_health_exp_pct_gdp": round(float(np.mean(chex_values)), 2),
            "median_health_exp_pct_gdp": round(median_chex, 2),
            "n_chex_obs": len(chex_values),
        }

        if median_chex <= 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "Health expenditure is zero or negative",
                "metrics": metrics,
            }

        # Cost-effectiveness ratio: life expectancy years per % of GDP spent on health
        ce_ratio = median_le / median_chex
        metrics["le_per_health_exp_pct_gdp"] = round(ce_ratio, 3)

        # Benchmark: 14 years of LE per % of GDP = moderate efficiency (score 50 raw)
        # (e.g., 70 years LE / 5% GDP = 14 ratio)
        ce_score = float(np.clip((ce_ratio / 14.0) * 50.0, 0, 100))
        # Invert: low efficiency = high stress
        score = float(np.clip(100.0 - ce_score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": metrics,
        }
