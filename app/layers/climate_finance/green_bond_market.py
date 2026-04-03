"""Green bond market: issuance as % of total bond market, growth trend, labeling integrity.

Methodology
-----------
**Green bond share** (Climate Bonds Initiative):
    green_bond_share = green_bond_issuance / total_bond_issuance * 100

    CBI tracks labeled green bonds by verified use-of-proceeds categories:
    renewable energy, energy efficiency, clean transport, sustainable water,
    green buildings, land use, climate adaptation.

**Growth trend**:
    Year-on-year growth in green bond issuance volume. Markets growing >20%/yr
    signal strong transition finance momentum.

**Labeling integrity** (greenwashing risk):
    Proxy = share of issuances with external review (second-party opinion,
    verification, or certification). Lower review rate raises greenwashing risk
    and reduces signal quality.

Score interpretation: higher score = larger, faster-growing, credible green
bond market. Low score = nascent or underdeveloped green finance.

Sources: Climate Bonds Initiative, BIS, World Bank, SIFMA
"""

from app.layers.base import LayerBase

_SQL = """
    SELECT dp.date, dp.value
    FROM data_points dp
    JOIN data_series ds ON dp.series_id = ds.id
    WHERE ds.code = ?
    ORDER BY dp.date
"""


class GreenBondMarket(LayerBase):
    layer_id = "lGF"
    name = "Green Bond Market"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "WLD")

        codes = {
            "green_issuance": f"GREEN_BOND_ISSUANCE_{country}",
            "total_issuance": f"TOTAL_BOND_ISSUANCE_{country}",
            "review_share": f"GREEN_BOND_REVIEW_SHARE_{country}",
        }

        data: dict[str, dict] = {}
        for key, code in codes.items():
            rows = await db.fetch_all(_SQL, (code,))
            if rows:
                data[key] = {r["date"]: float(r["value"]) for r in rows}

        if "green_issuance" not in data or "total_issuance" not in data:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No green bond or total bond issuance data",
            }

        common = sorted(set(data["green_issuance"]) & set(data["total_issuance"]))
        if not common:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No overlapping dates for green vs total bond issuance",
            }

        latest = common[-1]
        green = data["green_issuance"][latest]
        total = data["total_issuance"][latest]

        if total <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "Total bond issuance is zero"}

        share_pct = green / total * 100

        # Year-on-year growth
        yoy_growth = None
        if len(common) >= 2:
            prev = common[-2]
            prev_green = data["green_issuance"].get(prev)
            if prev_green and prev_green > 0:
                yoy_growth = (green - prev_green) / prev_green * 100

        # Labeling integrity
        review_rate = None
        if "review_share" in data:
            rv = list(data["review_share"].values())
            review_rate = float(rv[-1]) if rv else None

        # Score: share (0-50 pts) + growth (0-30 pts) + integrity (0-20 pts)
        score = min(share_pct * 5, 50)  # 10% share = 50 pts

        if yoy_growth is not None:
            score += min(max(yoy_growth, 0) * 0.5, 30)  # 60% growth = 30 pts

        if review_rate is not None:
            score += review_rate * 0.20  # 100% review = 20 pts
        else:
            score += 10  # assume moderate integrity if unknown

        score = min(max(score, 0), 100)

        return {
            "score": round(score, 1),
            "metrics": {
                "country": country,
                "reference_date": latest,
                "green_bond_issuance_usd_bn": round(green, 2),
                "total_bond_issuance_usd_bn": round(total, 2),
                "green_share_pct": round(share_pct, 2),
                "yoy_growth_pct": round(yoy_growth, 1) if yoy_growth is not None else None,
                "external_review_rate_pct": round(review_rate, 1) if review_rate is not None else None,
            },
        }
