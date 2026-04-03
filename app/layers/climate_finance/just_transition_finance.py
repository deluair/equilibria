"""Just transition finance: social spending to offset decarbonization impacts.

Methodology
-----------
**Just Transition concept** (ILO, 2015 Paris Agreement preamble):
    Decarbonization imposes concentrated costs on fossil-fuel-dependent workers
    and communities. Just transition finance channels resources to:
    - Worker retraining and reskilling programs
    - Social protection for displaced workers (extended unemployment, pensions)
    - Community economic diversification (industrial policy for coal regions)
    - Energy affordability for low-income households (carbon dividend)

**Just Transition Finance mechanisms**:
    1. Sovereign: EU Just Transition Fund (€55B), US Inflation Reduction Act
       (energy community bonus credits), South Africa JETP ($8.5B)
    2. MDB/DFI: World Bank PROGREEN, IFC green finance, EBRD
    3. Carbon revenue recycling: % of ETS/carbon tax revenue earmarked for JT

**Adequacy metrics**:
    JT adequacy = just_transition_spending / (affected_workers * avg_income)
    Displacement coverage = workers_with_support / workers_displaced * 100

**Fossil employment exposure**:
    share of total employment in coal, oil, gas extraction and refining.
    High exposure + low JT spending = high vulnerability.

Score: high fossil dependency with low JT spending raises score (crisis).

Sources: ILO Just Transition guidelines, EU JTF, OECD JT Assessment,
CPI Global Landscape of Climate Finance, IRENA
"""

from app.layers.base import LayerBase

_SQL = """
    SELECT dp.date, dp.value
    FROM data_points dp
    JOIN data_series ds ON dp.series_id = ds.id
    WHERE ds.code = ?
    ORDER BY dp.date
"""


class JustTransitionFinance(LayerBase):
    layer_id = "lGF"
    name = "Just Transition Finance"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "WLD")

        codes = {
            "jt_spending": f"JUST_TRANSITION_SPENDING_{country}",
            "fossil_employment_share": f"FOSSIL_EMPLOYMENT_SHARE_{country}",
            "workers_displaced": f"FOSSIL_WORKERS_DISPLACED_{country}",
            "workers_with_support": f"JT_WORKERS_SUPPORTED_{country}",
            "carbon_revenue_jt_share": f"CARBON_REVENUE_JT_SHARE_{country}",
            "gdp": f"GDP_{country}",
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

        jt_spending = latest_val("jt_spending")
        fossil_emp_share = latest_val("fossil_employment_share")
        displaced = latest_val("workers_displaced")
        supported = latest_val("workers_with_support")
        carbon_jt_share = latest_val("carbon_revenue_jt_share")
        gdp = latest_val("gdp")

        if jt_spending is None and fossil_emp_share is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No just transition spending or fossil employment data",
            }

        # Exposure score: high fossil employment share = more JT needed
        exposure_score = 0.0
        if fossil_emp_share is not None:
            exposure_score = min(fossil_emp_share * 5, 40)  # 8% share = 40 pts

        # Coverage deficit: workers displaced vs supported
        coverage_deficit_score = 20.0  # default: assume partial coverage unknown
        if displaced is not None and supported is not None and displaced > 0:
            coverage_rate = min(supported / displaced, 1.0)
            coverage_deficit_score = (1 - coverage_rate) * 30

        # Spending adequacy: JT spending as % GDP
        spending_deficit_score = 0.0
        if jt_spending is not None and gdp is not None and gdp > 0:
            jt_pct_gdp = jt_spending / gdp * 100
            # 1% GDP JT = adequate for most countries; below 0.2% = crisis
            spending_deficit_score = max(1 - jt_pct_gdp, 0) * 30
        elif jt_spending is None:
            spending_deficit_score = 30  # no data = assume no spending

        # Carbon revenue recycling bonus: higher share -> less score
        recycling_credit = 0.0
        if carbon_jt_share is not None:
            recycling_credit = min(carbon_jt_share * 0.1, 10)

        score = min(exposure_score + coverage_deficit_score + spending_deficit_score - recycling_credit, 100)
        score = max(score, 0)

        jt_pct_gdp_out = None
        if jt_spending is not None and gdp is not None and gdp > 0:
            jt_pct_gdp_out = jt_spending / gdp * 100

        coverage_rate_out = None
        if displaced is not None and supported is not None and displaced > 0:
            coverage_rate_out = min(supported / displaced * 100, 100)

        return {
            "score": round(score, 1),
            "metrics": {
                "country": country,
                "just_transition_spending_usd_bn": round(jt_spending, 3) if jt_spending is not None else None,
                "jt_spending_pct_gdp": round(jt_pct_gdp_out, 3) if jt_pct_gdp_out is not None else None,
                "fossil_employment_share_pct": round(fossil_emp_share, 2) if fossil_emp_share is not None else None,
                "workers_displaced": int(displaced) if displaced is not None else None,
                "workers_with_support": int(supported) if supported is not None else None,
                "displacement_coverage_rate_pct": round(coverage_rate_out, 1) if coverage_rate_out is not None else None,
                "carbon_revenue_jt_share_pct": round(carbon_jt_share, 1) if carbon_jt_share is not None else None,
            },
        }
