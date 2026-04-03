"""Green taxonomy alignment: sustainable finance share of total finance.

Methodology
-----------
**Green taxonomy frameworks** (EU Taxonomy Regulation 2020/852):
    Classifies economic activities by environmental objective:
    1. Climate change mitigation
    2. Climate change adaptation
    3. Sustainable use/protection of water/marine resources
    4. Transition to a circular economy
    5. Pollution prevention and control
    6. Protection of biodiversity and ecosystems

    DNSH (Do No Significant Harm) criteria apply across all objectives.
    Minimum social safeguards must be met.

**Taxonomy alignment rate**:
    Share of eligible activities that are taxonomy-aligned (fully compliant).
    EU large companies must disclose under CSRD/SFDR.

    alignment_rate = taxonomy_aligned_capex / total_capex * 100

**Sustainable finance share**:
    Broader measure including all ESG-labeled instruments:
    green bonds + sustainability-linked bonds + social bonds + sustainability bonds
    as % of total capital market issuance.

**Greenwashing risk**:
    Gap between self-reported and externally-verified alignment.
    High gap = greenwashing risk, lower effective score.

Score: high alignment = low score (healthy). Very low alignment with
high greenwashing risk = crisis.

Sources: EU Platform on Sustainable Finance, SFDR, CSRD, IOSCO,
CBI Sustainable Debt Market Summary
"""

from app.layers.base import LayerBase

_SQL = """
    SELECT dp.date, dp.value
    FROM data_points dp
    JOIN data_series ds ON dp.series_id = ds.id
    WHERE ds.code = ?
    ORDER BY dp.date
"""


class GreenTaxonomyAlignment(LayerBase):
    layer_id = "lGF"
    name = "Green Taxonomy Alignment"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "WLD")

        codes = {
            "taxonomy_alignment_pct": f"TAXONOMY_ALIGNMENT_PCT_{country}",
            "sustainable_finance_share": f"SUSTAINABLE_FINANCE_SHARE_{country}",
            "esg_aum_share": f"ESG_AUM_SHARE_{country}",
            "verified_green_share": f"VERIFIED_GREEN_SHARE_{country}",
            "self_reported_green_share": f"SELF_REPORTED_GREEN_SHARE_{country}",
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

        taxonomy_align = latest_val("taxonomy_alignment_pct")
        sf_share = latest_val("sustainable_finance_share")
        esg_aum = latest_val("esg_aum_share")
        verified = latest_val("verified_green_share")
        self_reported = latest_val("self_reported_green_share")

        if taxonomy_align is None and sf_share is None and esg_aum is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No taxonomy alignment or sustainable finance share data",
            }

        # Primary metric: taxonomy alignment or sustainable finance share
        primary = taxonomy_align if taxonomy_align is not None else sf_share
        if primary is None:
            primary = esg_aum or 0.0

        # Low alignment = high score (bad)
        # 100% aligned = 0; 0% = 70 pts
        alignment_score = max(100 - primary, 0) * 0.70

        # Greenwashing gap penalty
        greenwash_penalty = 0.0
        if verified is not None and self_reported is not None and self_reported > 0:
            gap = max(self_reported - verified, 0)
            greenwash_penalty = min(gap * 1.5, 30)

        score = min(alignment_score + greenwash_penalty, 100)

        return {
            "score": round(score, 1),
            "metrics": {
                "country": country,
                "taxonomy_alignment_pct": round(taxonomy_align, 2) if taxonomy_align is not None else None,
                "sustainable_finance_share_pct": round(sf_share, 2) if sf_share is not None else None,
                "esg_aum_share_pct": round(esg_aum, 2) if esg_aum is not None else None,
                "verified_green_share_pct": round(verified, 2) if verified is not None else None,
                "self_reported_green_share_pct": round(self_reported, 2) if self_reported is not None else None,
                "greenwashing_gap_pct": (
                    round(max(self_reported - verified, 0), 2)
                    if verified is not None and self_reported is not None
                    else None
                ),
            },
        }
