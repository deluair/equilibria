"""Digital trade measurement and barriers analysis.

Methodology:
    Measure the scope and restrictiveness of digital trade using four
    complementary approaches:

    1. Digitally deliverable services (DDS):
       Services that can be delivered remotely via ICT networks, following
       UNCTAD classification. Includes: insurance, financial, telecom,
       computer, business, audiovisual, IP charges, personal/cultural.
       DDS share = DDS exports / total services exports.

    2. ICT goods trade:
       Hardware and equipment enabling digital connectivity. HS chapters
       84-85 subsets (computers, telecom equipment, semiconductors,
       consumer electronics). Compute revealed tech advantage (RTA).

    3. Digital Trade Restrictiveness Index (DTRI):
       Composite of barriers following ECIPE methodology across:
       fiscal restrictions (tariffs on ICT goods, digital taxes),
       establishment restrictions (FDI limits in telecom/digital),
       data restrictions (localization, cross-border flow limits),
       trading restrictions (intermediary liability, content filtering).

    4. E-commerce penetration:
       B2C and B2B e-commerce as share of GDP. Internet penetration rate.
       Digital payments adoption.

    Score (0-100): Higher score indicates greater digital trade vulnerability
    (high barriers, low DDS share, weak ICT infrastructure).

References:
    UNCTAD (2015). "International Trade in ICT Services and ICT-enabled
        Services." Technical Notes on ICT for Development No. 3.
    Ferracane, M. & van der Marel, E. (2021). "Do Data Flows Restrictions
        Inhibit Trade in Services?" Review of World Economics, 157, 727-757.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DigitalTrade(LayerBase):
    layer_id = "l1"
    name = "Digital Trade"

    # UNCTAD digitally deliverable services categories
    DDS_CATEGORIES = frozenset({
        "insurance",
        "financial",
        "telecommunications",
        "computer",
        "other_business",
        "audiovisual",
        "intellectual_property",
        "personal_cultural",
    })

    # ICT goods HS codes (simplified prefixes)
    ICT_HS_PREFIXES = ("8471", "8473", "8517", "8523", "8525", "8528", "8541", "8542")

    async def compute(self, db, **kwargs) -> dict:
        """Compute digital trade indicators.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default BGD)
            year : int - reference year (optional)
        """
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""

        # --- Digitally deliverable services ---
        params: list = [country]
        if year:
            params.append(str(year))

        services_rows = await db.fetch_all(
            f"""
            SELECT ds.metadata AS category, dp.date,
                   SUM(dp.value) AS export_value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'services_trade'
              AND ds.country_iso3 = ?
              AND ds.name LIKE '%export%'
              {year_clause}
            GROUP BY ds.metadata, dp.date
            """,
            tuple(params),
        )

        dds_exports = 0.0
        total_services_exports = 0.0
        dds_by_category = {}

        if services_rows:
            latest = max(r["date"] for r in services_rows)
            for r in services_rows:
                if r["date"] == latest and r["export_value"]:
                    val = float(r["export_value"])
                    total_services_exports += val
                    cat = r["category"]
                    if cat in self.DDS_CATEGORIES:
                        dds_exports += val
                        dds_by_category[cat] = val

        dds_share = dds_exports / total_services_exports if total_services_exports > 0 else 0.0

        # DDS shares within digitally deliverable
        dds_composition = {}
        if dds_exports > 0:
            dds_composition = {k: v / dds_exports for k, v in dds_by_category.items()}

        # --- ICT goods trade ---
        ict_params: list = [country]
        if year:
            ict_params.append(str(year))

        ict_rows = await db.fetch_all(
            f"""
            SELECT ds.series_id AS product_code, dp.date,
                   SUM(dp.value) AS export_value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.country_iso3 = ?
              AND ds.name LIKE '%export%'
              {year_clause}
            GROUP BY ds.series_id, dp.date
            """,
            tuple(ict_params),
        )

        ict_exports = 0.0
        total_goods_exports = 0.0
        ict_by_product = {}

        if ict_rows:
            latest_goods = max(r["date"] for r in ict_rows)
            for r in ict_rows:
                if r["date"] == latest_goods and r["export_value"]:
                    val = float(r["export_value"])
                    total_goods_exports += val
                    code = str(r["product_code"])
                    if any(code.startswith(p) for p in self.ICT_HS_PREFIXES):
                        ict_exports += val
                        ict_by_product[code] = val

        ict_share = ict_exports / total_goods_exports if total_goods_exports > 0 else 0.0

        # Revealed technology advantage (RTA) for ICT goods
        # Fetch world ICT exports
        world_ict_params: list = []
        if year:
            world_ict_params.append(str(year))

        world_ict_rows = await db.fetch_all(
            f"""
            SELECT ds.series_id AS product_code, dp.date,
                   SUM(dp.value) AS export_value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.name LIKE '%export%'
              {year_clause}
            GROUP BY ds.series_id, dp.date
            """,
            tuple(world_ict_params),
        )

        world_ict_exports = 0.0
        world_total_exports = 0.0
        if world_ict_rows:
            latest_w = max(r["date"] for r in world_ict_rows)
            for r in world_ict_rows:
                if r["date"] == latest_w and r["export_value"]:
                    val = float(r["export_value"])
                    world_total_exports += val
                    code = str(r["product_code"])
                    if any(code.startswith(p) for p in self.ICT_HS_PREFIXES):
                        world_ict_exports += val

        world_ict_share = (
            world_ict_exports / world_total_exports if world_total_exports > 0 else 0.0
        )
        rta = ict_share / world_ict_share if world_ict_share > 0 else 0.0

        # --- Digital trade barriers ---
        dtri_rows = await db.fetch_all(
            """
            SELECT ds.metadata AS barrier_type, dp.value AS barrier_score
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'dtri'
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        barriers = {}
        dtri_composite = None
        if dtri_rows:
            for r in dtri_rows:
                if r["barrier_score"] is not None:
                    barriers[r["barrier_type"]] = float(r["barrier_score"])
            if barriers:
                dtri_composite = float(np.mean(list(barriers.values())))

        # --- E-commerce and digital infrastructure ---
        ecom_rows = await db.fetch_all(
            """
            SELECT ds.name, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('wdi', 'itu')
              AND ds.country_iso3 = ?
              AND (ds.name LIKE '%internet%' OR ds.name LIKE '%ecommerce%'
                   OR ds.name LIKE '%digital%payment%'
                   OR ds.name LIKE '%ICT%')
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        digital_infra = {}
        for r in ecom_rows:
            if r["value"] is not None:
                digital_infra[r["name"]] = float(r["value"])

        internet_penetration = digital_infra.get("internet_users_pct", None)

        # --- Score computation ---
        # Low DDS share = vulnerability (max 30 points)
        # DDS share for advanced economies is typically 50-70%
        dds_penalty = (1.0 - min(dds_share / 0.7, 1.0)) * 30.0

        # Low ICT goods competitiveness (max 20 points)
        ict_penalty = (1.0 - min(rta / 2.0, 1.0)) * 20.0

        # High barriers (max 30 points)
        barrier_penalty = (dtri_composite or 0.3) * 30.0  # DTRI is 0-1

        # Low internet penetration (max 20 points)
        inet_pct = (internet_penetration or 30.0) / 100.0
        infra_penalty = (1.0 - min(inet_pct, 1.0)) * 20.0

        score = float(np.clip(
            dds_penalty + ict_penalty + barrier_penalty + infra_penalty,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "dds_exports": round(dds_exports, 2),
            "total_services_exports": round(total_services_exports, 2),
            "dds_share": round(dds_share, 4),
            "dds_composition": {k: round(v, 4) for k, v in dds_composition.items()},
            "ict_goods_exports": round(ict_exports, 2),
            "ict_goods_share": round(ict_share, 4),
            "revealed_tech_advantage": round(rta, 4),
            "dtri_composite": round(dtri_composite, 4) if dtri_composite else None,
            "barriers_by_type": {k: round(v, 4) for k, v in barriers.items()}
            if barriers else None,
            "internet_penetration": internet_penetration,
            "digital_infrastructure": digital_infra if digital_infra else None,
        }
