"""Optimal Currency Area (OCA): readiness for monetary union assessment.

Methodology
-----------
Mundell (1961) original OCA criteria and subsequent extensions
(McKinnon 1963, Kenen 1969, Frankel & Rose 1998):

  1. Trade openness (McKinnon): high trade share -> gains from exchange rate
     stability -> OCA candidate. (NE.TRD.GNFS.ZS)

  2. Labor mobility proxy (Mundell): labor force growth rate differential as
     a rough proxy for factor mobility. Converging rates -> adjustment capacity.
     (SL.TLF.TOTL.IN, YoY growth rate)

  3. Business cycle synchronization (Frankel-Rose): correlation of GDP growth
     rates between country and reference region.
     (NY.GDP.MKTP.KD.ZG)

  4. Composite OCA score: weighted average of normalized criteria.
     Higher OCA score -> better suited for monetary union -> lower score stress.

Composite OCA index:
    OCA = 0.40 * openness_norm + 0.30 * labor_norm + 0.30 * sync_norm
    (each normalized to [0, 1])

Score = 100 - OCA * 100 (lower OCA readiness = higher stress)

Sources: World Bank WDI
  NE.TRD.GNFS.ZS  - Trade (% of GDP)
  SL.TLF.TOTL.IN  - Labor force, total
  NY.GDP.MKTP.KD.ZG - GDP growth (constant prices, %)
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class OptimalCurrencyArea(LayerBase):
    layer_id = "l15"
    name = "Optimal Currency Area"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        reference_country = kwargs.get("reference_country", "DEU")
        lookback = kwargs.get("lookback_years", 20)

        series_map = {
            "trade_openness": f"NE.TRD.GNFS.ZS_{country}",
            "labor_force": f"SL.TLF.TOTL.IN_{country}",
            "gdp_growth": f"NY.GDP.MKTP.KD.ZG_{country}",
            "ref_gdp_growth": f"NY.GDP.MKTP.KD.ZG_{reference_country}",
        }

        data: dict[str, dict[str, float]] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE series_id = ?) "
                "AND date >= date('now', ?) ORDER BY date",
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        results: dict = {
            "country": country,
            "reference_country": reference_country,
        }
        criteria: dict[str, float | None] = {}

        # --- 1. Trade openness ---
        openness_score: float | None = None
        if data.get("trade_openness"):
            td = sorted(data["trade_openness"])
            trade_vals = np.array([data["trade_openness"][d] for d in td])
            mean_openness = float(np.mean(trade_vals))
            # Normalize: 0% = 0, 100% = 0.5, 150%+ = 1.0
            openness_score = float(np.clip(mean_openness / 150.0, 0.0, 1.0))
            results["trade_openness"] = {
                "mean_pct_gdp": round(mean_openness, 3),
                "latest_pct_gdp": round(float(trade_vals[-1]), 3),
                "n_obs": len(trade_vals),
                "normalized_score": round(openness_score, 4),
            }
            criteria["openness"] = openness_score

        # --- 2. Labor mobility proxy (convergence of labor force growth rates) ---
        labor_score: float | None = None
        if data.get("labor_force"):
            ld = sorted(data["labor_force"])
            lf_vals = np.array([data["labor_force"][d] for d in ld])
            if len(lf_vals) >= 3:
                lf_growth = np.diff(lf_vals) / np.maximum(np.abs(lf_vals[:-1]), 1e-6) * 100.0
                lf_cv = float(np.std(lf_growth, ddof=1)) / max(abs(float(np.mean(lf_growth))), 0.01)
                # Low CV -> stable labor supply -> better mobility proxy
                labor_score = float(np.clip(1.0 - min(lf_cv / 2.0, 1.0), 0.0, 1.0))
                results["labor_mobility_proxy"] = {
                    "labor_force_growth_mean_pct": round(float(np.mean(lf_growth)), 4),
                    "labor_force_growth_std_pct": round(float(np.std(lf_growth, ddof=1)), 4),
                    "cv": round(lf_cv, 4),
                    "normalized_score": round(labor_score, 4),
                }
                criteria["labor"] = labor_score

        # --- 3. Business cycle synchronization ---
        sync_score: float | None = None
        if data.get("gdp_growth") and data.get("ref_gdp_growth"):
            common = sorted(set(data["gdp_growth"]) & set(data["ref_gdp_growth"]))
            if len(common) >= 5:
                y1 = np.array([data["gdp_growth"][d] for d in common])
                y2 = np.array([data["ref_gdp_growth"][d] for d in common])
                if np.std(y1, ddof=1) > 1e-10 and np.std(y2, ddof=1) > 1e-10:
                    corr, p_val = sp_stats.pearsonr(y1, y2)
                    # Map correlation [-1, 1] to [0, 1]
                    sync_score = float(np.clip((corr + 1.0) / 2.0, 0.0, 1.0))
                    results["business_cycle_sync"] = {
                        "correlation": round(float(corr), 4),
                        "p_value": round(float(p_val), 4),
                        "significant": float(p_val) < 0.10,
                        "n_obs": len(common),
                        "normalized_score": round(sync_score, 4),
                    }
                    criteria["sync"] = sync_score

        # --- Composite OCA index ---
        weights = {"openness": 0.40, "labor": 0.30, "sync": 0.30}
        available = {k: v for k, v in criteria.items() if v is not None}

        if not available:
            return {"score": 50.0, "results": {"error": "no OCA criteria data available"}}

        # Renormalize weights to available criteria
        total_weight = sum(weights[k] for k in available)
        oca_composite = sum(weights[k] / total_weight * v for k, v in available.items())

        results["oca_composite"] = round(oca_composite, 4)
        results["oca_readiness"] = (
            "high" if oca_composite > 0.70
            else "moderate" if oca_composite > 0.40
            else "low"
        )
        results["criteria_available"] = list(available.keys())

        # Score: lower OCA readiness = higher stress
        score = float(np.clip((1.0 - oca_composite) * 100.0, 0.0, 100.0))

        return {"score": round(score, 1), "results": results}
