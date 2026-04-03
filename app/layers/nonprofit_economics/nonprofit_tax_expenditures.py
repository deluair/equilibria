"""Nonprofit tax expenditures: tax revenue foregone via charitable deductions.

Governments forgo revenue through tax-exempt status and charitable deduction
incentives for nonprofits. The fiscal cost (tax expenditure) is proxied via
tax revenue as % of GDP (GC.TAX.TOTL.GD.ZS). Low tax revenue capacity
implies a weak base from which tax expenditures are feasible; very high
revenue with low nonprofit giving suggests foregone revenue is large.
The ratio of tax revenue to private giving proxies the relative fiscal cost.

Score: very low tax revenue -> CRISIS (no fiscal space for exemptions);
high tax revenue + active giving -> STRESS (large fiscal cost).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class NonprofitTaxExpenditures(LayerBase):
    layer_id = "lNP"
    name = "Nonprofit Tax Expenditures"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        tax_code = "GC.TAX.TOTL.GD.ZS"
        transfer_code = "BX.TRF.PWKR.DT.GD.ZS"

        tax_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (tax_code, "%tax revenue%"),
        )
        transfer_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (transfer_code, "%personal remittances received%"),
        )

        tax_vals = [r["value"] for r in tax_rows if r["value"] is not None]
        transfer_vals = [r["value"] for r in transfer_rows if r["value"] is not None]

        if not tax_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for GC.TAX.TOTL.GD.ZS (tax revenue)",
            }

        tax_gdp = tax_vals[0]
        transfers_gdp = transfer_vals[0] if transfer_vals else 0.0
        trend = round(tax_vals[0] - tax_vals[-1], 3) if len(tax_vals) > 1 else None

        # Tax expenditure proxy: estimated foregone revenue = transfers_gdp * marginal_rate_proxy
        # Marginal rate proxy scaled from tax/GDP ratio (higher taxes = higher marginal rates)
        marginal_rate_proxy = tax_gdp / 100.0 * 0.35  # approximate top marginal rate fraction
        implied_tax_expenditure = transfers_gdp * marginal_rate_proxy

        # Score: very low tax capacity -> CRISIS; moderate tax + high expenditure -> STRESS
        if tax_gdp < 8.0:
            score = 70.0 + (8.0 - tax_gdp) * 2.5  # crisis range: thin fiscal base
        elif tax_gdp < 15.0:
            score = 45.0 + (15.0 - tax_gdp) * 3.57
        elif tax_gdp < 25.0:
            score = 20.0 + (tax_gdp - 15.0) * 1.0 + implied_tax_expenditure * 10.0
        else:
            score = min(100.0, 30.0 + (tax_gdp - 25.0) * 1.5 + implied_tax_expenditure * 15.0)

        score = min(100.0, max(0.0, score))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "tax_revenue_gdp_pct": round(tax_gdp, 2),
                "private_transfers_gdp_pct": round(transfers_gdp, 3),
                "implied_tax_expenditure_gdp_pct": round(implied_tax_expenditure, 4),
                "trend_tax_change": trend,
                "n_obs_tax": len(tax_vals),
                "n_obs_transfers": len(transfer_vals),
            },
        }
