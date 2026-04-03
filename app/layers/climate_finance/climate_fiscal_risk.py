"""Climate fiscal risk: fiscal exposure to climate shocks and stranded assets.

Methodology
-----------
**Climate fiscal risk** (IMF Fiscal Monitor 2020, OECD 2021):
    Government balance sheets are exposed to climate through:

    1. **Stranded asset risk**: fiscal revenue loss from devalued fossil fuel
       assets (royalties, taxes, state-owned enterprise dividends).
       stranded_revenue_risk = fossil_revenue_pct_gdp * expected_stranding_rate

    2. **Physical shock risk**: fiscal cost of disaster relief, public
       infrastructure replacement, healthcare surge from climate-health impacts.
       physical_fiscal_cost = (nat_cat_loss_pct_gdp * uninsured_ratio
                               * government_coverage_share)

    3. **Transition shock risk**: abrupt policy tightening imposes sudden
       costs on fossil-dependent economies (terms of trade, unemployment,
       social spending surge). Carbon sudden stop scenario.

    4. **Debt sustainability under climate**: climate-adjusted debt trajectory
       under 2C and 4C warming scenarios. Higher warming = larger fiscal gaps.

**Vulnerability index**:
    Combined fiscal climate vulnerability = weighted sum of above components.
    High fossil revenue dependence + high physical risk + low fiscal buffers
    = maximum vulnerability.

**Fiscal space for climate**:
    Whether government has room to increase climate spending without
    triggering debt distress (IMF debt sustainability threshold).

Score: high fiscal exposure to climate risks raises score.

Sources: IMF Fiscal Monitor Oct 2020, OECD Climate-Related Fiscal Risks,
World Bank Climate Change Action Plan, V-Lab climate stress tests
"""

import numpy as np

from app.layers.base import LayerBase

_SQL = """
    SELECT dp.date, dp.value
    FROM data_points dp
    JOIN data_series ds ON dp.series_id = ds.id
    WHERE ds.code = ?
    ORDER BY dp.date
"""


class ClimateFiscalRisk(LayerBase):
    layer_id = "lGF"
    name = "Climate Fiscal Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "WLD")

        codes = {
            "fossil_revenue_pct_gdp": f"FOSSIL_FISCAL_REVENUE_PCT_GDP_{country}",
            "nat_cat_fiscal_cost_pct_gdp": f"NAT_CAT_FISCAL_COST_PCT_GDP_{country}",
            "debt_pct_gdp": f"DEBT_PCT_GDP_{country}",
            "fiscal_balance_pct_gdp": f"FISCAL_BALANCE_PCT_GDP_{country}",
            "state_fossil_assets_pct_gdp": f"STATE_FOSSIL_ASSETS_PCT_GDP_{country}",
            "climate_spending_pct_gdp": f"CLIMATE_SPENDING_PCT_GDP_{country}",
        }

        data: dict[str, dict] = {}
        for key, code in codes.items():
            rows = await db.fetch_all(_SQL, (code,))
            if rows:
                data[key] = {r["date"]: float(r["value"]) for r in rows}

        def latest_val(key: str) -> float | None:
            if key not in data:
                return None
            vals = list(data[key].values())
            return float(vals[-1]) if vals else None

        fossil_rev = latest_val("fossil_revenue_pct_gdp")
        nat_cat_cost = latest_val("nat_cat_fiscal_cost_pct_gdp")
        debt = latest_val("debt_pct_gdp")
        fiscal_balance = latest_val("fiscal_balance_pct_gdp")
        state_fossil = latest_val("state_fossil_assets_pct_gdp")
        climate_spending = latest_val("climate_spending_pct_gdp")

        if fossil_rev is None and nat_cat_cost is None and debt is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No climate fiscal risk data available",
            }

        # Stranded asset revenue risk (0-35 pts)
        # Assumes 30% stranding rate under NZE scenario over 20 years
        stranded_risk_score = 0.0
        if fossil_rev is not None:
            stranded_risk_score = min(fossil_rev * 3.5, 35)
        if state_fossil is not None:
            stranded_risk_score += min(state_fossil * 0.5, 10)
        stranded_risk_score = min(stranded_risk_score, 35)

        # Physical fiscal shock risk (0-30 pts)
        physical_score = 0.0
        if nat_cat_cost is not None:
            physical_score = min(nat_cat_cost * 10, 30)

        # Fiscal buffer adequacy: can government absorb climate shocks? (0-25 pts)
        buffer_score = 0.0
        if debt is not None:
            # Debt >90% GDP = limited space; <30% = ample
            buffer_score += min(max(debt - 30, 0) / 60 * 15, 15)
        if fiscal_balance is not None:
            # Negative balance = less space
            if fiscal_balance < 0:
                buffer_score += min(abs(fiscal_balance) * 1.5, 10)

        # Climate spending effort (reduces score, up to -10 pts)
        spending_credit = 0.0
        if climate_spending is not None:
            spending_credit = min(climate_spending * 5, 10)

        score = min(stranded_risk_score + physical_score + buffer_score - spending_credit, 100)
        score = max(score, 0)

        # Composite risk level
        risk_level = "LOW" if score < 25 else "MODERATE" if score < 50 else "HIGH" if score < 75 else "SEVERE"

        return {
            "score": round(score, 1),
            "metrics": {
                "country": country,
                "fossil_revenue_pct_gdp": round(fossil_rev, 2) if fossil_rev is not None else None,
                "state_fossil_assets_pct_gdp": round(state_fossil, 2) if state_fossil is not None else None,
                "nat_cat_fiscal_cost_pct_gdp": round(nat_cat_cost, 3) if nat_cat_cost is not None else None,
                "debt_pct_gdp": round(debt, 1) if debt is not None else None,
                "fiscal_balance_pct_gdp": round(fiscal_balance, 2) if fiscal_balance is not None else None,
                "climate_spending_pct_gdp": round(climate_spending, 3) if climate_spending is not None else None,
                "stranded_risk_score": round(stranded_risk_score, 1),
                "physical_fiscal_score": round(physical_score, 1),
                "buffer_score": round(buffer_score, 1),
                "risk_level": risk_level,
            },
        }
