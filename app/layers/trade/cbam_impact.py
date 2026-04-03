"""EU Carbon Border Adjustment Mechanism (CBAM) impact estimation.

The EU CBAM (effective 2026 transitional, 2034 full implementation) imposes
a carbon price on imports of carbon-intensive goods (cement, iron/steel,
aluminium, fertilizers, electricity, hydrogen) equivalent to the EU ETS
carbon price.

CBAM tariff equivalent for product k from country j:
    t_CBAM_jk = (e_jk - e_EU_k) * P_CO2 / P_jk

where:
- e_jk = embodied carbon intensity of product k from country j (tCO2/unit)
- e_EU_k = EU reference emission intensity
- P_CO2 = EU ETS carbon price (EUR/tCO2)
- P_jk = unit price of product k from country j

Impact channels:
1. Direct cost: additional tariff on covered exports to EU
2. Competitiveness: relative cost shift vs. EU and cleaner competitors
3. Trade diversion: exports redirected from EU to non-CBAM markets
4. Domestic policy: incentive to adopt carbon pricing (CBAM credit)

The score reflects CBAM exposure: high share of covered products in EU
exports, high carbon intensity gap, and limited ability to divert.
"""

from app.layers.base import LayerBase

# CBAM-covered HS chapters (approximate)
CBAM_HS_CHAPTERS = {
    "25": "cement",       # HS 2523 specifically, but broad chapter
    "26": "iron_ore",
    "27": "electricity",  # HS 2716
    "28": "hydrogen",     # HS 2804
    "31": "fertilizers",
    "72": "iron_steel",
    "73": "iron_steel_articles",
    "76": "aluminium",
}

# Default carbon intensity benchmarks (tCO2 per tonne of product)
# These are approximate global averages from IEA/OECD data
DEFAULT_INTENSITIES = {
    "cement": {"global_avg": 0.63, "eu_benchmark": 0.55},
    "iron_steel": {"global_avg": 1.85, "eu_benchmark": 1.52},
    "aluminium": {"global_avg": 8.60, "eu_benchmark": 1.60},
    "fertilizers": {"global_avg": 2.10, "eu_benchmark": 1.80},
    "electricity": {"global_avg": 0.48, "eu_benchmark": 0.27},
    "hydrogen": {"global_avg": 9.30, "eu_benchmark": 3.40},
}


class CBAMImpact(LayerBase):
    layer_id = "l1"
    name = "CBAM Impact"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        carbon_price = kwargs.get("carbon_price_eur", 80.0)  # EU ETS price
        year = kwargs.get("year")

        year_filter = "AND dp.date = ?" if year else ""

        # Fetch country's exports to EU (partner = EU or specific EU members)
        eu_partners = (
            "DEU", "FRA", "ITA", "ESP", "NLD", "BEL", "POL", "SWE", "AUT",
            "IRL", "DNK", "FIN", "PRT", "CZE", "ROU", "GRC", "HUN",
        )
        eu_placeholders = ",".join("?" * len(eu_partners))

        params: list = [country]
        params.extend(eu_partners)
        if year:
            params.append(str(year))

        eu_exports = await db.fetch_all(
            f"""
            SELECT ds.series_id AS product_code, ds.description AS partner,
                   SUM(dp.value) AS value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.country_iso3 = ?
              AND ds.description IN ({eu_placeholders})
              AND ds.name LIKE '%export%'
              AND dp.value > 0
              {year_filter}
            GROUP BY ds.series_id
            """,
            tuple(params),
        )

        # Also get total exports for share computation
        total_params: list = [country]
        if year:
            total_params.append(str(year))

        total_exports = await db.fetch_all(
            f"""
            SELECT SUM(dp.value) AS total_value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.country_iso3 = ?
              AND ds.name LIKE '%export%'
              AND dp.value > 0
              {year_filter}
            """,
            tuple(total_params),
        )

        total_export_value = (
            total_exports[0]["total_value"]
            if total_exports and total_exports[0]["total_value"]
            else 0
        )

        if not eu_exports:
            return {
                "score": 5.0,
                "country": country,
                "signal": "STABLE",
                "note": "no EU exports found, CBAM exposure minimal",
            }

        # Classify exports as CBAM-covered or not
        covered_value = 0.0
        uncovered_value = 0.0
        covered_by_sector: dict[str, float] = {}

        for row in eu_exports:
            product = str(row["product_code"])
            value = row["value"]
            hs2 = product[:2] if len(product) >= 2 else product

            if hs2 in CBAM_HS_CHAPTERS:
                sector = CBAM_HS_CHAPTERS[hs2]
                covered_value += value
                covered_by_sector[sector] = covered_by_sector.get(sector, 0) + value
            else:
                uncovered_value += value

        total_eu_exports = covered_value + uncovered_value

        # Compute tariff equivalent for each covered sector
        sector_impacts = []
        total_cbam_cost = 0.0

        for sector, value in covered_by_sector.items():
            intensities = DEFAULT_INTENSITIES.get(sector)
            if not intensities:
                continue

            carbon_gap = intensities["global_avg"] - intensities["eu_benchmark"]
            # Tariff equivalent as share of value (rough approximation)
            # Assumes ~$500/tonne average product value for metals, adjust by sector
            avg_prices = {
                "cement": 80, "iron_steel": 600, "aluminium": 2500,
                "fertilizers": 400, "electricity": 50, "hydrogen": 3000,
            }
            avg_price = avg_prices.get(sector, 500)

            tariff_equiv = (carbon_gap * carbon_price) / avg_price if avg_price > 0 else 0
            cbam_cost = value * tariff_equiv

            sector_impacts.append({
                "sector": sector,
                "export_value": round(value, 2),
                "carbon_intensity_gap": round(carbon_gap, 3),
                "tariff_equivalent_pct": round(tariff_equiv * 100, 2),
                "estimated_cbam_cost": round(cbam_cost, 2),
                "share_of_eu_exports": round(value / total_eu_exports, 4) if total_eu_exports > 0 else 0,
            })
            total_cbam_cost += cbam_cost

        # Coverage ratio
        coverage_ratio = covered_value / total_eu_exports if total_eu_exports > 0 else 0
        eu_share_of_total = total_eu_exports / total_export_value if total_export_value > 0 else 0

        # Trade diversion potential: how much of covered exports could go elsewhere?
        diversion_potential = min(1.0, 1.0 - coverage_ratio)  # Simplified

        # Score: high coverage + high carbon gap + low diversion potential = high impact
        coverage_score = coverage_ratio * 40.0
        cost_score = min(30.0, (total_cbam_cost / max(total_eu_exports, 1)) * 1000.0)
        diversion_score = (1.0 - diversion_potential) * 30.0
        score = max(0.0, min(100.0, coverage_score + cost_score + diversion_score))

        return {
            "score": round(score, 2),
            "country": country,
            "carbon_price_eur": carbon_price,
            "eu_export_summary": {
                "total_eu_exports": round(total_eu_exports, 2),
                "cbam_covered_value": round(covered_value, 2),
                "uncovered_value": round(uncovered_value, 2),
                "coverage_ratio": round(coverage_ratio, 4),
                "eu_share_of_total_exports": round(eu_share_of_total, 4),
            },
            "total_estimated_cbam_cost": round(total_cbam_cost, 2),
            "cbam_cost_as_share_of_eu_exports": (
                round(total_cbam_cost / total_eu_exports, 6) if total_eu_exports > 0 else 0
            ),
            "sector_impacts": sorted(sector_impacts, key=lambda x: x["estimated_cbam_cost"], reverse=True),
            "diversion_potential": round(diversion_potential, 4),
        }
