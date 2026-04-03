"""Equity risk premium estimation.

The ERP is the excess return investors demand for holding equities over a
risk-free asset. Estimated here as the difference between the earnings yield
(inverse of P/E, proxied by stock market return NY.ADJ.DKAP.GN.ZS or market
return series) and the long-term government bond yield (FR.INR.LNDF).

High ERP can signal distress (rising risk aversion) or attractive valuations.
A very low or negative ERP signals overvaluation.

Score (0-100): moderate ERP (4-6%) = low stress. Very high (>10%) or negative
(<0%) = elevated stress.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EquityRiskPremium(LayerBase):
    layer_id = "lCK"
    name = "Equity Risk Premium"

    # Benchmark ERP range considered "normal" (Damodaran, 2023 estimates)
    ERP_LOW_NORMAL = 4.0   # %
    ERP_HIGH_NORMAL = 7.0  # %

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
                "error": "no interest rate or equity return data",
            }

        by_code: dict[str, list[float]] = {}
        for r in rows:
            by_code.setdefault(r["indicator_code"], []).append(float(r["value"]))

        # Risk-free rate: prefer lending rate, fall back to real interest rate
        if "FR.INR.LNDF" in by_code:
            rfr_vals = np.array(by_code["FR.INR.LNDF"])
            rfr_label = "lending_rate"
        elif "FR.INR.RINR" in by_code:
            rfr_vals = np.array(by_code["FR.INR.RINR"])
            rfr_label = "real_interest_rate"
        else:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no risk-free rate series available",
            }

        rfr_latest = float(rfr_vals[-1])

        # Equity return: stock market index annual change
        erp_estimate = None
        equity_return = None
        if "CM.MKT.INDX.ZG" in by_code:
            eq_vals = np.array(by_code["CM.MKT.INDX.ZG"])
            equity_return = float(np.mean(eq_vals))
            erp_estimate = equity_return - rfr_latest

        if erp_estimate is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "equity return data unavailable for ERP calculation",
            }

        # Score: stress rises outside normal ERP band
        if self.ERP_LOW_NORMAL <= erp_estimate <= self.ERP_HIGH_NORMAL:
            score = 20.0  # Normal band = low stress
        elif erp_estimate < 0:
            score = 80.0  # Negative ERP: overvaluation risk
        elif erp_estimate > self.ERP_HIGH_NORMAL:
            # Very high ERP = high risk aversion
            excess = erp_estimate - self.ERP_HIGH_NORMAL
            score = float(np.clip(30.0 + excess * 5.0, 30.0, 100.0))
        else:
            # Below normal
            gap = self.ERP_LOW_NORMAL - erp_estimate
            score = float(np.clip(20.0 + gap * 10.0, 20.0, 60.0))

        return {
            "score": round(score, 2),
            "country": country,
            "equity_risk_premium": {
                "erp_pct": round(erp_estimate, 2),
                "equity_return_mean_pct": round(equity_return, 2),
                "risk_free_rate_pct": round(rfr_latest, 2),
                "rfr_series": rfr_label,
            },
            "valuation_signal": (
                "overvalued" if erp_estimate < 0
                else "fairly_valued" if self.ERP_LOW_NORMAL <= erp_estimate <= self.ERP_HIGH_NORMAL
                else "high_risk_aversion" if erp_estimate > self.ERP_HIGH_NORMAL
                else "low_premium"
            ),
            "benchmark_range_pct": [self.ERP_LOW_NORMAL, self.ERP_HIGH_NORMAL],
        }
