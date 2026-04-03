"""Climate risk in financial sector: physical and transition risk exposure.

Methodology
-----------
**Physical risk exposure** (NGFS, ECB 2021 climate stress test):
    Physical risk = financial assets exposed to acute hazards (floods, storms,
    wildfires, heat) + chronic hazards (sea-level rise, changing precipitation).

    Physical risk score = sum_i (exposure_i * hazard_probability_i * asset_value_i)
                         / total_financial_assets

    Key metrics:
    - Real estate loan portfolio share in high-flood-risk areas
    - Corporate loan share in climate-exposed sectors (agriculture, tourism, coastal)
    - Insurance underwriting losses from natural catastrophes as % premiums

**Transition risk exposure** (NGFS):
    Stranded asset risk from policy-driven devaluation of fossil fuel assets.
    Brown assets = coal, oil, gas reserves + related infrastructure.

    Transition risk score = brown_assets / total_bank_assets
    Amplified by carbon price uncertainty (regulatory risk premium).

    Decomposition:
    - Direct fossil exposure: bank loans to oil/gas/coal
    - Indirect: exposure to energy-intensive industries facing carbon costs
    - Securities portfolio: fossil fuel equity/bond holdings

**TCFD alignment**:
    Share of large financial institutions disclosing climate risks per TCFD
    framework. Low disclosure -> higher uncertainty.

Score: higher combined physical + transition risk raises score.

Sources: NGFS, ECB, Federal Reserve, BIS, TCFD 2023 Status Report
"""

from app.layers.base import LayerBase

_SQL = """
    SELECT dp.date, dp.value
    FROM data_points dp
    JOIN data_series ds ON dp.series_id = ds.id
    WHERE ds.code = ?
    ORDER BY dp.date
"""


class ClimateRiskFinancial(LayerBase):
    layer_id = "lGF"
    name = "Climate Risk Financial Sector"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "WLD")

        codes = {
            "physical_risk_exposure": f"FINANCIAL_PHYSICAL_RISK_PCT_{country}",
            "transition_risk_exposure": f"FINANCIAL_TRANSITION_RISK_PCT_{country}",
            "brown_loans_pct": f"BROWN_LOANS_PCT_TOTAL_{country}",
            "nat_cat_loss_ratio": f"NAT_CAT_LOSS_RATIO_{country}",
            "tcfd_disclosure_pct": f"TCFD_DISCLOSURE_PCT_{country}",
            "fossil_securities_pct": f"FOSSIL_SECURITIES_PCT_{country}",
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

        physical = latest_val("physical_risk_exposure")
        transition = latest_val("transition_risk_exposure")
        brown_loans = latest_val("brown_loans_pct")
        nat_cat_ratio = latest_val("nat_cat_loss_ratio")
        tcfd_pct = latest_val("tcfd_disclosure_pct")
        fossil_sec = latest_val("fossil_securities_pct")

        if physical is None and transition is None and brown_loans is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No financial climate risk data available",
            }

        # Physical risk component (0-45 pts)
        physical_score = 0.0
        if physical is not None:
            physical_score = min(physical * 2, 30)
        if nat_cat_ratio is not None:
            physical_score += min(nat_cat_ratio * 15, 15)

        # Transition risk component (0-40 pts)
        transition_score = 0.0
        if transition is not None:
            transition_score = min(transition * 2, 25)
        if brown_loans is not None:
            transition_score += min(brown_loans * 0.5, 10)
        if fossil_sec is not None:
            transition_score += min(fossil_sec * 0.5, 5)

        # Disclosure gap penalty (0-15 pts): low TCFD disclosure = less transparency
        disclosure_penalty = 0.0
        if tcfd_pct is not None:
            disclosure_penalty = max(100 - tcfd_pct, 0) * 0.15

        score = min(physical_score + transition_score + disclosure_penalty, 100)

        return {
            "score": round(score, 1),
            "metrics": {
                "country": country,
                "physical_risk_exposure_pct": round(physical, 2) if physical is not None else None,
                "transition_risk_exposure_pct": round(transition, 2) if transition is not None else None,
                "brown_loans_pct_total": round(brown_loans, 2) if brown_loans is not None else None,
                "nat_cat_loss_ratio": round(nat_cat_ratio, 3) if nat_cat_ratio is not None else None,
                "tcfd_disclosure_pct": round(tcfd_pct, 1) if tcfd_pct is not None else None,
                "fossil_securities_pct": round(fossil_sec, 2) if fossil_sec is not None else None,
                "physical_risk_score": round(physical_score, 1),
                "transition_risk_score": round(transition_score, 1),
            },
        }
