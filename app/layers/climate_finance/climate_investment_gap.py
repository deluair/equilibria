"""Climate investment gap: needed vs actual climate investment, technology decomposition.

Methodology
-----------
**Climate investment need** (IEA Net Zero Emissions scenario):
    Annual investment required to align with NZE2050 pathway, decomposed by:
    - Clean power (solar, wind, nuclear, hydro, storage)
    - End-use electrification (transport, buildings, industry)
    - Efficiency improvements
    - Low-carbon fuels (hydrogen, biofuels)
    - Carbon capture and storage

    IEA NZE requires ~$4 trillion/yr globally by 2030, rising to $4.5T by 2050,
    vs ~$1.8 trillion currently. McKinsey Global Energy Perspective 2023 aligns.

**Investment gap** (IPCC AR6 Ch.15):
    gap = required_annual - actual_annual
    gap_pct_gdp = gap / gdp * 100
    adequacy_ratio = actual / required  (1.0 = fully aligned)

**Technology decomposition**:
    Tracks where investment is concentrated vs where gaps are largest.
    Adaptation finance typically receives <10% of total climate finance
    despite accounting for ~50% of total need.

Score: low adequacy ratio raises score (worse = higher gap). A fully-funded
climate investment pipeline scores 0; complete absence scores 100.

Sources: IEA World Energy Investment, CPI Global Landscape of Climate Finance,
BloombergNEF Energy Transition Investment Trends
"""

from app.layers.base import LayerBase

_SQL = """
    SELECT dp.date, dp.value
    FROM data_points dp
    JOIN data_series ds ON dp.series_id = ds.id
    WHERE ds.code = ?
    ORDER BY dp.date
"""


class ClimateInvestmentGap(LayerBase):
    layer_id = "lGF"
    name = "Climate Investment Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "WLD")

        codes = {
            "actual_investment": f"CLIMATE_FINANCE_ACTUAL_{country}",
            "required_investment": f"CLIMATE_FINANCE_REQUIRED_{country}",
            "gdp": f"GDP_{country}",
            "mitigation_share": f"CLIMATE_MITIGATION_SHARE_{country}",
            "adaptation_share": f"CLIMATE_ADAPTATION_SHARE_{country}",
        }

        data: dict[str, dict] = {}
        for key, code in codes.items():
            rows = await db.fetch_all(_SQL, (code,))
            if rows:
                data[key] = {r["date"]: float(r["value"]) for r in rows}

        if "actual_investment" not in data or "required_investment" not in data:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No climate investment data (actual or required)",
            }

        common = sorted(set(data["actual_investment"]) & set(data["required_investment"]))
        if not common:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No overlapping dates for actual vs required investment",
            }

        latest = common[-1]
        actual = data["actual_investment"][latest]
        required = data["required_investment"][latest]

        if required <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "Required investment is zero"}

        adequacy_ratio = actual / required
        gap = max(required - actual, 0)

        gap_pct_gdp = None
        if "gdp" in data:
            gdp_vals = list(data["gdp"].values())
            gdp = float(gdp_vals[-1]) if gdp_vals else None
            if gdp and gdp > 0:
                gap_pct_gdp = gap / gdp * 100

        mitigation_share = None
        if "mitigation_share" in data:
            mv = list(data["mitigation_share"].values())
            mitigation_share = float(mv[-1]) if mv else None

        adaptation_share = None
        if "adaptation_share" in data:
            av = list(data["adaptation_share"].values())
            adaptation_share = float(av[-1]) if av else None

        # Score: high gap = high score (crisis)
        # adequacy_ratio 1.0 = score 0; ratio 0.0 = score 100
        gap_score = max(1 - adequacy_ratio, 0) * 80

        # Penalty if adaptation chronically underfunded (<10% of total)
        adaptation_penalty = 0.0
        if adaptation_share is not None and adaptation_share < 10:
            adaptation_penalty = (10 - adaptation_share) * 2  # up to 20 pts

        score = min(gap_score + adaptation_penalty, 100)

        return {
            "score": round(score, 1),
            "metrics": {
                "country": country,
                "reference_date": latest,
                "actual_investment_usd_bn": round(actual, 2),
                "required_investment_usd_bn": round(required, 2),
                "investment_gap_usd_bn": round(gap, 2),
                "adequacy_ratio": round(adequacy_ratio, 3),
                "gap_pct_gdp": round(gap_pct_gdp, 2) if gap_pct_gdp is not None else None,
                "mitigation_share_pct": round(mitigation_share, 1) if mitigation_share is not None else None,
                "adaptation_share_pct": round(adaptation_share, 1) if adaptation_share is not None else None,
            },
        }
