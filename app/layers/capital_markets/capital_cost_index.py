"""Capital cost index (WACC estimate).

The weighted average cost of capital (WACC) determines whether investment
projects are economically viable. A high WACC relative to peers discourages
investment and signals financial market inefficiency.

Estimated as: WACC ~ (D/V) * Kd * (1 - tax) + (E/V) * Ke
where Kd = lending rate (FR.INR.LNDF), Ke = risk-free rate + ERP proxy,
D/V assumed 0.6 (typical EM leverage), tax rate assumed 0.25.

Score (0-100): high WACC vs. benchmark = high cost of capital = stress.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CapitalCostIndex(LayerBase):
    layer_id = "lCK"
    name = "Capital Cost Index"

    DEBT_WEIGHT = 0.60
    EQUITY_WEIGHT = 0.40
    TAX_RATE = 0.25
    ERP_ASSUMED = 5.0  # % equity risk premium when not computable
    WACC_BENCHMARK = 10.0  # % peer-group benchmark for emerging markets

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('FR.INR.LNDF', 'FR.INR.RINR', 'CM.MKT.INDX.ZG')
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
                "error": "no interest rate data for WACC estimation",
            }

        by_code: dict[str, list[float]] = {}
        for r in rows:
            by_code.setdefault(r["indicator_code"], []).append(float(r["value"]))

        # Cost of debt
        if "FR.INR.LNDF" in by_code:
            kd_vals = np.array(by_code["FR.INR.LNDF"])
            kd = float(kd_vals[-1])
            kd_label = "lending_rate"
        elif "FR.INR.RINR" in by_code:
            kd_vals = np.array(by_code["FR.INR.RINR"])
            kd = float(kd_vals[-1])
            kd_label = "real_interest_rate"
        else:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no lending rate available",
            }

        # Cost of equity: risk-free + ERP
        rf = kd  # Lending rate as risk-free proxy (conservative)
        if "CM.MKT.INDX.ZG" in by_code:
            eq_vals = np.array(by_code["CM.MKT.INDX.ZG"])
            eq_return = float(np.mean(eq_vals))
            erp = max(eq_return - rf, 0.0)
        else:
            erp = self.ERP_ASSUMED
        ke = rf + erp

        # WACC
        wacc = (self.DEBT_WEIGHT * kd * (1.0 - self.TAX_RATE)) + (self.EQUITY_WEIGHT * ke)

        # Score: excess WACC above benchmark = stress
        excess = max(wacc - self.WACC_BENCHMARK, 0.0)
        score = float(np.clip(excess * 5.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "wacc_estimate": {
                "wacc_pct": round(wacc, 2),
                "cost_of_debt_pct": round(kd, 2),
                "cost_of_equity_pct": round(ke, 2),
                "equity_risk_premium_pct": round(erp, 2),
                "kd_series": kd_label,
                "assumptions": {
                    "debt_weight": self.DEBT_WEIGHT,
                    "equity_weight": self.EQUITY_WEIGHT,
                    "tax_rate": self.TAX_RATE,
                },
            },
            "vs_benchmark": {
                "benchmark_pct": self.WACC_BENCHMARK,
                "excess_pp": round(wacc - self.WACC_BENCHMARK, 2),
            },
            "cost_level": (
                "low" if wacc < self.WACC_BENCHMARK
                else "elevated" if wacc < self.WACC_BENCHMARK + 5
                else "high"
            ),
        }
