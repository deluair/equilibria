"""Public goods provision and valuation.

Implements core public goods theory:

1. Samuelson (1954) condition for optimal provision of a pure public good.
   At optimum, the sum of marginal rates of substitution across all
   individuals equals the marginal rate of transformation:
       sum_i(MRS_i) = MRT
   Deviation from this condition indicates under- or over-provision.

2. Contingent valuation (CV): estimates willingness-to-pay (WTP) for
   non-market goods (environment, defense, parks) via stated preferences.
   Applies the Turnbull lower bound estimator and parametric models.

3. Benefit-cost analysis framework: net present value of public projects
   with shadow pricing and distributional weights.

4. Social discount rate selection: Ramsey rule
       r = delta + eta * g
   where delta is pure time preference, eta is elasticity of marginal
   utility, g is per-capita consumption growth. Typical range 1-5%.

References:
    Samuelson, P. (1954). The Pure Theory of Public Expenditure.
        Review of Economics and Statistics, 36(4), 387-389.
    Arrow, K. et al. (1993). Report of the NOAA Panel on Contingent
        Valuation. Federal Register, 58(10), 4601-4614.
    Boardman, A., Greenberg, D., Vining, A. & Weimer, D. (2018).
        Cost-Benefit Analysis: Concepts and Practice. 5th ed.
    Ramsey, F. (1928). A Mathematical Theory of Saving.
        Economic Journal, 38(152), 543-559.

Sources: WDI (government expenditure), FRED (discount rates)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


def _ramsey_discount_rate(
    pure_time_preference: float,
    utility_elasticity: float,
    consumption_growth: float,
) -> float:
    """Ramsey rule: r = delta + eta * g."""
    return pure_time_preference + utility_elasticity * consumption_growth


def _turnbull_wtp(bids: np.ndarray, acceptance_rates: np.ndarray) -> float:
    """Turnbull lower-bound estimator for mean WTP.

    Uses the step-function approach: expected WTP is the sum of bid
    increments weighted by the fraction accepting at each threshold.

    Returns estimated mean WTP.
    """
    order = np.argsort(bids)
    bids = bids[order]
    rates = acceptance_rates[order]

    # Enforce monotonicity (acceptance should decrease with bid)
    mono_rates = np.copy(rates)
    for i in range(1, len(mono_rates)):
        if mono_rates[i] > mono_rates[i - 1]:
            mono_rates[i] = mono_rates[i - 1]

    # Turnbull lower bound
    wtp = 0.0
    for i in range(len(bids)):
        if i == 0:
            prob = 1.0 - mono_rates[0]
        else:
            prob = mono_rates[i - 1] - mono_rates[i]
        prob = max(0.0, prob)
        wtp += bids[i] * prob

    # Add tail: last acceptance rate times last bid
    if len(bids) > 0 and mono_rates[-1] > 0:
        wtp += bids[-1] * mono_rates[-1]

    return float(wtp)


class PublicGoods(LayerBase):
    layer_id = "l10"
    name = "Public Goods Provision"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        results = {"country": country}

        # --- Fetch government expenditure data ---
        gov_exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN (
                'NE.CON.GOVT.ZS', 'GC.XPN.TOTL.GD.ZS', 'GOV_EXP_GDP'
              )
            ORDER BY dp.date
            """,
            (country,),
        )

        gdp_growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('NY.GDP.PCAP.KD.ZG', 'GDPPC_GROWTH')
            ORDER BY dp.date
            """,
            (country,),
        )

        # --- Samuelson condition assessment ---
        # With aggregate data, we approximate by comparing government
        # expenditure share to benchmarks. The Samuelson condition requires
        # sum(MRS) = MRT. We proxy MRS by revealed preference from spending.
        gov_exp_pct = None
        if gov_exp_rows:
            values = [float(r["value"]) for r in gov_exp_rows if r["value"]]
            if values:
                gov_exp_pct = values[-1]

        # Cross-country comparison for provision adequacy
        all_gov_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN ('NE.CON.GOVT.ZS', 'GC.XPN.TOTL.GD.ZS')
              AND dp.value > 0
            ORDER BY dp.date DESC
            """
        )

        cross_country = {}
        for r in all_gov_rows:
            iso = r["country_iso3"]
            if iso not in cross_country:
                cross_country[iso] = float(r["value"])

        samuelson = {}
        if cross_country and len(cross_country) >= 10:
            all_shares = np.array(list(cross_country.values()))
            median_share = float(np.median(all_shares))
            p25 = float(np.percentile(all_shares, 25))
            p75 = float(np.percentile(all_shares, 75))

            if gov_exp_pct is not None:
                # Deviation from median (proxy for optimal under Samuelson)
                deviation = gov_exp_pct - median_share
                percentile_rank = float(np.searchsorted(np.sort(all_shares), gov_exp_pct) / len(all_shares) * 100)
                samuelson = {
                    "gov_expenditure_pct_gdp": round(gov_exp_pct, 2),
                    "cross_country_median": round(median_share, 2),
                    "cross_country_p25": round(p25, 2),
                    "cross_country_p75": round(p75, 2),
                    "deviation_from_median": round(deviation, 2),
                    "percentile_rank": round(percentile_rank, 1),
                    "assessment": ("under-provision" if gov_exp_pct < p25 else "over-provision" if gov_exp_pct > p75 else "adequate"),
                    "n_countries": len(cross_country),
                }
            else:
                samuelson = {"error": "no expenditure data for target country"}
        else:
            samuelson = {"error": "insufficient cross-country data"}

        results["samuelson"] = samuelson

        # --- Contingent valuation (with stored survey data if available) ---
        cv_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id LIKE '%WTP%'
            ORDER BY dp.value
            """,
            (country,),
        )

        if cv_rows and len(cv_rows) >= 3:
            import json

            bids = []
            accept_rates = []
            for r in cv_rows:
                meta = json.loads(r["metadata"]) if r.get("metadata") else {}
                bid = float(r["value"])
                rate = meta.get("acceptance_rate")
                if rate is not None:
                    bids.append(bid)
                    accept_rates.append(float(rate))

            if len(bids) >= 3:
                bids_arr = np.array(bids)
                rates_arr = np.array(accept_rates)
                mean_wtp = _turnbull_wtp(bids_arr, rates_arr)
                results["contingent_valuation"] = {
                    "n_bid_levels": len(bids),
                    "mean_wtp_turnbull": round(mean_wtp, 2),
                    "bid_range": [round(float(bids_arr.min()), 2), round(float(bids_arr.max()), 2)],
                }
            else:
                results["contingent_valuation"] = {"error": "insufficient bid data"}
        else:
            results["contingent_valuation"] = {"error": "no WTP survey data"}

        # --- Benefit-cost analysis framework ---
        # Social discount rate via Ramsey rule
        delta = kwargs.get("pure_time_preference", 0.015)
        eta = kwargs.get("utility_elasticity", 1.5)

        avg_growth = 0.02  # default
        if gdp_growth_rows:
            growths = [float(r["value"]) / 100.0 for r in gdp_growth_rows if r["value"]]
            if growths:
                avg_growth = float(np.mean(growths[-10:]))  # last 10 years

        sdr = _ramsey_discount_rate(delta, eta, avg_growth)

        # Compare with market rates
        market_rate_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('GS10', 'FR.INR.LEND', 'LONG_RATE')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        market_rate = float(market_rate_rows[0]["value"]) / 100.0 if market_rate_rows else None

        bca = {
            "ramsey_discount_rate": round(sdr, 4),
            "components": {
                "pure_time_preference": delta,
                "utility_elasticity": eta,
                "consumption_growth": round(avg_growth, 4),
            },
            "market_rate": round(market_rate, 4) if market_rate else None,
            "sdr_vs_market": (round(sdr - market_rate, 4) if market_rate else None),
        }

        # Example BCA: evaluate a hypothetical project with given parameters
        project_cost = kwargs.get("project_cost")
        project_benefits = kwargs.get("project_annual_benefit")
        project_years = kwargs.get("project_years", 30)

        if project_cost and project_benefits:
            discount_factors = np.array([(1.0 + sdr) ** (-t) for t in range(project_years + 1)])
            pv_benefits = project_benefits * discount_factors[1:].sum()
            pv_costs = float(project_cost)
            npv = pv_benefits - pv_costs
            bcr = pv_benefits / pv_costs if pv_costs > 0 else 0

            bca["project_evaluation"] = {
                "pv_benefits": round(pv_benefits, 2),
                "pv_costs": round(pv_costs, 2),
                "npv": round(npv, 2),
                "bcr": round(bcr, 3),
                "recommendation": "accept" if npv > 0 else "reject",
            }

        results["benefit_cost"] = bca

        # --- Score ---
        score = 30.0

        # Under/over-provision
        if samuelson.get("assessment") == "under-provision":
            score += 25
        elif samuelson.get("assessment") == "over-provision":
            score += 15

        # Discount rate too high suggests underinvestment in public goods
        if sdr > 0.06:
            score += 15
        elif sdr > 0.04:
            score += 8

        # Large gap between social and market rate
        if bca.get("sdr_vs_market") is not None:
            gap = abs(bca["sdr_vs_market"])
            if gap > 0.03:
                score += 10

        score = max(0.0, min(100.0, score))

        return {"score": round(score, 1), "results": results}
