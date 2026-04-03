"""Trade in services decomposition using BPM6 categories.

Methodology:
    Decompose services trade into BPM6 (Balance of Payments Manual 6th ed.)
    categories: transport, travel, telecommunications, financial, insurance,
    and intellectual property. Compute:

    1. Services RCA (Balassa index adapted to services):
       SRCA_ik = (SX_ik / SX_i) / (SX_wk / SX_w)
       where SX = services exports, i = country, k = service category.

    2. Services Trade Restrictiveness Index (STRI):
       Composite of policy barriers across sectors following OECD methodology.
       Five policy areas: restrictions on foreign entry, restrictions on
       movement of people, other discriminatory measures, barriers to
       competition, regulatory transparency.

    3. Services trade balance decomposition: identify surplus/deficit
       categories and structural shifts over time.

    Score (0-100): Higher score indicates greater vulnerability in services
    trade (high restrictiveness, low diversification, declining RCA in
    key sectors).

References:
    Hoekman, B. & Mattoo, A. (2008). "Services Trade and Growth." World Bank
        Policy Research Working Paper 4461.
    Nordas, H.K. & Rouzet, D. (2017). "The Impact of Services Trade
        Restrictiveness on Trade Flows." The World Economy, 40(6), 1155-1183.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# BPM6 services categories
BPM6_CATEGORIES = [
    "transport",
    "travel",
    "telecommunications",
    "financial",
    "insurance",
    "intellectual_property",
    "other_business",
    "construction",
    "government",
    "personal_cultural",
    "manufacturing_on_inputs",
    "maintenance_repair",
]


class TradeInServices(LayerBase):
    layer_id = "l1"
    name = "Trade in Services"

    async def compute(self, db, **kwargs) -> dict:
        """Compute BPM6 services trade decomposition, SRCA, and STRI.

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

        # Fetch country services exports by category
        params: list = [country]
        if year:
            params.append(str(year))

        country_rows = await db.fetch_all(
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

        if not country_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no services trade data"}

        # Fetch world services exports by category
        world_params: list = []
        if year:
            world_params.append(str(year))

        world_rows = await db.fetch_all(
            f"""
            SELECT ds.metadata AS category, dp.date,
                   SUM(dp.value) AS export_value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'services_trade'
              AND ds.name LIKE '%export%'
              {year_clause}
            GROUP BY ds.metadata, dp.date
            """,
            tuple(world_params),
        )

        # Fetch country services imports for balance
        import_params: list = [country]
        if year:
            import_params.append(str(year))

        import_rows = await db.fetch_all(
            f"""
            SELECT ds.metadata AS category, dp.date,
                   SUM(dp.value) AS import_value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'services_trade'
              AND ds.country_iso3 = ?
              AND ds.name LIKE '%import%'
              {year_clause}
            GROUP BY ds.metadata, dp.date
            """,
            tuple(import_params),
        )

        # Use latest available date
        dates = sorted({r["date"] for r in country_rows})
        latest_date = max(dates)

        # Country exports by category for latest date
        c_exports = {}
        for r in country_rows:
            if r["date"] == latest_date and r["export_value"]:
                c_exports[r["category"]] = float(r["export_value"])

        # World exports by category for latest date
        w_exports = {}
        for r in world_rows:
            if r["date"] == latest_date and r["export_value"]:
                w_exports[r["category"]] = float(r["export_value"])

        # Country imports by category for latest date
        c_imports = {}
        for r in import_rows:
            if r["date"] == latest_date and r["import_value"]:
                c_imports[r["category"]] = float(r["import_value"])

        total_c_exports = sum(c_exports.values()) if c_exports else 0
        total_w_exports = sum(w_exports.values()) if w_exports else 0

        # Services RCA by category
        srca = {}
        if total_c_exports > 0 and total_w_exports > 0:
            for cat, val in c_exports.items():
                wval = w_exports.get(cat, 0)
                if wval > 0:
                    srca[cat] = (val / total_c_exports) / (wval / total_w_exports)

        # Identify categories with comparative advantage
        advantage_cats = {c: v for c, v in srca.items() if v > 1.0}
        n_advantage = len(advantage_cats)
        n_total = len(srca) if srca else 1

        # BPM6 decomposition: export shares
        export_shares = {}
        if total_c_exports > 0:
            for cat, val in c_exports.items():
                export_shares[cat] = val / total_c_exports

        # Services trade balance by category
        balance_by_cat = {}
        total_imports = sum(c_imports.values()) if c_imports else 0
        for cat in set(list(c_exports.keys()) + list(c_imports.keys())):
            exp = c_exports.get(cat, 0)
            imp = c_imports.get(cat, 0)
            balance_by_cat[cat] = exp - imp

        services_balance = total_c_exports - total_imports

        # Concentration of services exports (HHI)
        if export_shares:
            shares_arr = np.array(list(export_shares.values()))
            hhi = float(np.sum(shares_arr ** 2))
        else:
            hhi = 1.0

        # Fetch STRI data if available
        stri_rows = await db.fetch_all(
            """
            SELECT ds.metadata AS sector, dp.value AS stri_value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'stri'
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 20
            """,
            (country,),
        )

        stri_by_sector = {}
        avg_stri = None
        if stri_rows:
            for r in stri_rows:
                if r["stri_value"] is not None:
                    stri_by_sector[r["sector"]] = float(r["stri_value"])
            if stri_by_sector:
                avg_stri = float(np.mean(list(stri_by_sector.values())))

        # Dynamic analysis: compare latest to previous period
        dynamic = None
        if len(dates) >= 2:
            prev_date = sorted(dates)[-2]
            prev_exports = {}
            for r in country_rows:
                if r["date"] == prev_date and r["export_value"]:
                    prev_exports[r["category"]] = float(r["export_value"])

            if prev_exports:
                total_prev = sum(prev_exports.values())
                growth_by_cat = {}
                for cat in c_exports:
                    curr = c_exports.get(cat, 0)
                    prev = prev_exports.get(cat, 0)
                    if prev > 0:
                        growth_by_cat[cat] = (curr - prev) / prev
                dynamic = {
                    "comparison_dates": [sorted(dates)[-2], latest_date],
                    "growth_by_category": {
                        k: round(v, 4) for k, v in growth_by_cat.items()
                    },
                    "total_growth": round(
                        (total_c_exports - total_prev) / total_prev, 4
                    ) if total_prev > 0 else None,
                }

        # Score: high restrictiveness + low diversification + deficit
        diversification_ratio = n_advantage / max(n_total, 1)
        concentration_penalty = hhi * 30.0
        diversification_penalty = (1.0 - diversification_ratio) * 30.0
        stri_penalty = (avg_stri or 0.3) * 40.0  # STRI is 0-1 scale
        score = float(np.clip(
            concentration_penalty + diversification_penalty + stri_penalty,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "date": latest_date,
            "total_services_exports": total_c_exports,
            "total_services_imports": total_imports,
            "services_balance": services_balance,
            "export_shares_bpm6": {k: round(v, 4) for k, v in export_shares.items()},
            "balance_by_category": {k: round(v, 2) for k, v in balance_by_cat.items()},
            "services_rca": {k: round(v, 4) for k, v in sorted(
                srca.items(), key=lambda x: x[1], reverse=True
            )},
            "n_categories_rca_above_1": n_advantage,
            "hhi_services": round(hhi, 4),
            "stri_by_sector": {k: round(v, 4) for k, v in stri_by_sector.items()}
            if stri_by_sector else None,
            "avg_stri": round(avg_stri, 4) if avg_stri is not None else None,
        }

        if dynamic:
            result["dynamic"] = dynamic

        return result
