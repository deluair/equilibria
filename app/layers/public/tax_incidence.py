"""Tax incidence and burden analysis.

Implements several canonical public finance models:

1. Harberger (1962) corporate tax incidence model. In a two-sector general
   equilibrium, the burden of a corporate tax depends on relative factor
   intensities and demand/supply elasticities. When capital is mobile across
   sectors, capital bears less and labor bears more of the burden.

   Key formula (Harberger two-sector):
       dK/dr = -t * f_K / [e_S * (1 + sigma_K/sigma_L) + e_D * (sigma_K/sigma_L)]
   where e_S, e_D are supply/demand elasticities, sigma are substitution elasticities.

2. VAT pass-through: the fraction of a VAT increase passed to consumers
   depends on supply/demand elasticity ratio:
       pass_through = e_S / (e_S + e_D)

3. Progressivity indices:
   - Kakwani (1977): K = concentration_coeff(tax) - gini(income)
     K > 0 progressive, K < 0 regressive
   - Suits (1977): S = 1 - 2 * area_under_relative_concentration_curve
     S > 0 progressive, S < 0 regressive

4. Laffer curve: tax revenue as a function of tax rate.
   R(t) = t * B(t) where B(t) is the tax base that shrinks with rate.
   Assuming constant-elasticity response: B(t) = B0 * (1 - t)^epsilon
   Revenue-maximizing rate: t* = 1 / (1 + epsilon)

References:
    Harberger, A. (1962). The Incidence of the Corporation Income Tax.
        Journal of Political Economy, 70(3), 215-240.
    Kakwani, N. (1977). Measurement of Tax Progressivity. Economic Journal.
    Suits, D. (1977). Measurement of Tax Progressivity. AER, 67(4), 747-752.
    Saez, E., Slemrod, J. & Giertz, S. (2012). The Elasticity of Taxable
        Income. Journal of Economic Literature, 50(1), 3-50.

Sources: FRED (tax revenue, GDP), WDI (tax/GDP ratio), IMF GFS
"""

from __future__ import annotations

import numpy as np
from scipy import integrate

from app.layers.base import LayerBase


def _gini(values: np.ndarray) -> float:
    """Gini coefficient from an array of non-negative values."""
    v = np.sort(values)
    n = len(v)
    if n == 0 or v.sum() == 0:
        return 0.0
    cumv = np.cumsum(v)
    return float(1.0 - 2.0 * cumv.sum() / (n * cumv[-1]) + 1.0 / n)


def _concentration_coefficient(values: np.ndarray, ranks: np.ndarray) -> float:
    """Concentration coefficient: like Gini but ordered by external ranking."""
    order = np.argsort(ranks)
    sorted_v = values[order]
    n = len(sorted_v)
    if n == 0 or sorted_v.sum() == 0:
        return 0.0
    cumv = np.cumsum(sorted_v)
    return float(1.0 - 2.0 * cumv.sum() / (n * cumv[-1]) + 1.0 / n)


def _laffer_revenue(tax_rate: float, base0: float, elasticity: float) -> float:
    """Revenue at given rate assuming constant-elasticity base response."""
    if tax_rate < 0 or tax_rate >= 1:
        return 0.0
    return tax_rate * base0 * (1.0 - tax_rate) ** elasticity


class TaxIncidence(LayerBase):
    layer_id = "l10"
    name = "Tax Incidence"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # --- Fetch tax and income data ---
        tax_rev_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('GC.TAX.TOTL.GD.ZS', 'TAX_REVENUE')
            ORDER BY dp.date
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('NY.GDP.MKTP.KD', 'GDP')
            ORDER BY dp.date
            """,
            (country,),
        )

        results = {"country": country}

        # --- Harberger corporate tax incidence ---
        # Calibrate with standard parameters
        sigma_k = kwargs.get("sigma_k", 1.0)  # capital substitution elasticity
        sigma_l = kwargs.get("sigma_l", 0.7)  # labor substitution elasticity
        e_supply = kwargs.get("e_supply", 1.5)  # capital supply elasticity
        e_demand = kwargs.get("e_demand", 0.8)  # output demand elasticity
        corp_tax_rate = kwargs.get("corp_tax_rate", 0.21)

        ratio = sigma_k / sigma_l if sigma_l > 0 else 1.0
        denom = e_supply * (1.0 + ratio) + e_demand * ratio
        capital_burden_share = e_demand * ratio / denom if denom > 0 else 0.5
        labor_burden_share = 1.0 - capital_burden_share

        results["harberger"] = {
            "corp_tax_rate": corp_tax_rate,
            "capital_burden_share": round(capital_burden_share, 4),
            "labor_burden_share": round(labor_burden_share, 4),
            "parameters": {
                "sigma_k": sigma_k,
                "sigma_l": sigma_l,
                "e_supply": e_supply,
                "e_demand": e_demand,
            },
        }

        # --- VAT pass-through ---
        vat_supply_e = kwargs.get("vat_supply_elasticity", 2.0)
        vat_demand_e = kwargs.get("vat_demand_elasticity", 0.5)
        vat_rate = kwargs.get("vat_rate", 0.15)

        consumer_pass = vat_supply_e / (vat_supply_e + vat_demand_e)
        producer_pass = 1.0 - consumer_pass
        effective_consumer_price_increase = vat_rate * consumer_pass

        results["vat_passthrough"] = {
            "vat_rate": vat_rate,
            "consumer_share": round(consumer_pass, 4),
            "producer_share": round(producer_pass, 4),
            "effective_consumer_increase": round(effective_consumer_price_increase, 4),
            "parameters": {
                "supply_elasticity": vat_supply_e,
                "demand_elasticity": vat_demand_e,
            },
        }

        # --- Progressivity indices ---
        income_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id LIKE '%INCOME_DECILE%'
            ORDER BY dp.value
            """,
            (country,),
        )

        tax_share_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id LIKE '%TAX_DECILE%'
            ORDER BY dp.value
            """,
            (country,),
        )

        if income_rows and tax_share_rows and len(income_rows) >= 5:
            incomes = np.array([float(r["value"]) for r in income_rows])
            taxes = np.array([float(r["value"]) for r in tax_share_rows[:len(incomes)]])

            gini_income = _gini(incomes)
            ranks = np.arange(len(incomes))
            conc_tax = _concentration_coefficient(taxes, ranks)

            kakwani = conc_tax - gini_income

            # Suits index: integrate relative concentration curve
            cum_income = np.cumsum(incomes) / incomes.sum()
            cum_tax = np.cumsum(taxes) / taxes.sum() if taxes.sum() > 0 else cum_income
            cum_income_ext = np.concatenate([[0], cum_income])
            cum_tax_ext = np.concatenate([[0], cum_tax])
            area = float(integrate.trapezoid(cum_tax_ext, cum_income_ext))
            suits = 1.0 - 2.0 * area

            progressive = kakwani > 0.05
            regressive = kakwani < -0.05

            results["progressivity"] = {
                "kakwani_index": round(kakwani, 4),
                "suits_index": round(suits, 4),
                "gini_income": round(gini_income, 4),
                "concentration_tax": round(conc_tax, 4),
                "classification": "progressive" if progressive else "regressive" if regressive else "proportional",
            }
        else:
            results["progressivity"] = {"error": "insufficient decile data"}

        # --- Laffer curve estimation ---
        taxable_income_elasticity = kwargs.get("taxable_income_elasticity", 0.4)
        tax_gdp = None
        if tax_rev_rows and gdp_rows:
            tax_dict = {r["date"]: float(r["value"]) for r in tax_rev_rows}
            gdp_dict = {r["date"]: float(r["value"]) for r in gdp_rows}
            common = sorted(set(tax_dict) & set(gdp_dict))
            if common:
                latest = common[-1]
                tax_gdp = tax_dict[latest] / gdp_dict[latest] if gdp_dict[latest] > 0 else None

        # Revenue-maximizing rate
        t_star = 1.0 / (1.0 + taxable_income_elasticity)

        # Laffer curve points
        rates = np.linspace(0, 0.95, 50)
        revenues = [_laffer_revenue(t, 1.0, taxable_income_elasticity) for t in rates]
        peak_rev = max(revenues)
        peak_rate = rates[np.argmax(revenues)]

        # Distance of current effective rate from peak
        current_rate = tax_gdp if tax_gdp and tax_gdp < 1 else corp_tax_rate
        current_rev = _laffer_revenue(current_rate, 1.0, taxable_income_elasticity)
        on_right_side = current_rate > t_star

        results["laffer"] = {
            "taxable_income_elasticity": taxable_income_elasticity,
            "revenue_maximizing_rate": round(t_star, 4),
            "current_effective_rate": round(current_rate, 4) if current_rate else None,
            "on_declining_side": on_right_side,
            "revenue_at_current": round(current_rev, 4),
            "peak_revenue_normalized": round(peak_rev, 4),
            "curve": {
                "rates": [round(float(r), 3) for r in rates[::5]],
                "revenues": [round(float(v), 4) for v in revenues[::5]],
            },
        }

        # --- Score ---
        # High score = tax system distortions / concerns
        score = 30.0  # baseline

        # Harberger: labor bearing >70% is concerning
        if labor_burden_share > 0.7:
            score += 15
        elif labor_burden_share > 0.6:
            score += 8

        # Regressive tax system
        if results.get("progressivity", {}).get("classification") == "regressive":
            score += 20
        elif results.get("progressivity", {}).get("classification") == "proportional":
            score += 5

        # Laffer: on declining side
        if on_right_side:
            score += 20

        # VAT: high consumer pass-through with high rate
        if consumer_pass > 0.8 and vat_rate > 0.15:
            score += 10

        score = max(0.0, min(100.0, score))

        return {"score": round(score, 1), "results": results}
