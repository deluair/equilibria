"""Trade complementarity index between country pairs.

The Trade Complementarity Index (TCI) measures how well one country's export
profile matches another country's import profile (Michaely 1996):

    TCI_ij = 100 * (1 - 0.5 * sum_k |m_jk - x_ik|)

where x_ik = share of product k in country i's total exports,
      m_jk = share of product k in country j's total imports.

TCI ranges from 0 (no overlap, completely mismatched) to 100 (perfect
complementarity: country i exports exactly what country j imports).

High complementarity suggests strong potential for bilateral trade
expansion.  Low complementarity indicates structural mismatch.

The module also computes:
- Asymmetric complementarity (i->j vs j->i)
- Complementarity dynamics over time
- Product-level contribution to complementarity

The score measures trade fit risk: low complementarity with major
partners signals weak trade relationships vulnerable to diversion.
"""

import numpy as np

from app.layers.base import LayerBase


class TradeComplementarity(LayerBase):
    layer_id = "l1"
    name = "Trade Complementarity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        partner = kwargs.get("partner_iso3")
        year = kwargs.get("year")

        year_filter = "AND dp.date = ?" if year else ""

        # Country's export shares by product
        c_params: list = [country]
        if year:
            c_params.append(str(year))

        country_exports = await db.fetch_all(
            f"""
            SELECT ds.series_id AS product_code, SUM(dp.value) AS value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.country_iso3 = ?
              AND ds.name LIKE '%export%'
              AND dp.value > 0
              {year_filter}
            GROUP BY ds.series_id
            """,
            tuple(c_params),
        )

        if not country_exports:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no export data"}

        # If specific partner, get their imports; otherwise check top partners
        partners_to_check = []
        if partner:
            partners_to_check = [partner]
        else:
            # Get top trading partners
            top_partners = await db.fetch_all(
                """
                SELECT DISTINCT ds.description AS partner_iso3
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.source IN ('baci', 'comtrade')
                  AND ds.country_iso3 = ?
                  AND ds.name LIKE '%export%'
                  AND ds.description IS NOT NULL
                GROUP BY ds.description
                ORDER BY SUM(dp.value) DESC
                LIMIT 10
                """,
                (country,),
            )
            partners_to_check = [r["partner_iso3"] for r in top_partners if r["partner_iso3"]]

        if not partners_to_check:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no partner data"}

        # Export shares for country
        total_exports = sum(r["value"] for r in country_exports)
        export_shares = {
            r["product_code"]: r["value"] / total_exports
            for r in country_exports
            if total_exports > 0
        }

        # Compute TCI for each partner
        results_by_partner = []
        for p in partners_to_check:
            p_params: list = [p]
            if year:
                p_params.append(str(year))

            partner_imports = await db.fetch_all(
                f"""
                SELECT ds.series_id AS product_code, SUM(dp.value) AS value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.source IN ('baci', 'comtrade')
                  AND ds.country_iso3 = ?
                  AND ds.name LIKE '%import%'
                  AND dp.value > 0
                  {year_filter}
                GROUP BY ds.series_id
                """,
                tuple(p_params),
            )

            if not partner_imports:
                continue

            total_imports = sum(r["value"] for r in partner_imports)
            import_shares = {
                r["product_code"]: r["value"] / total_imports
                for r in partner_imports
                if total_imports > 0
            }

            tci = self._compute_tci(export_shares, import_shares)

            # Product-level contributions (top 5 most complementary products)
            all_products = set(export_shares.keys()) | set(import_shares.keys())
            product_contributions = []
            for prod in all_products:
                xs = export_shares.get(prod, 0)
                ms = import_shares.get(prod, 0)
                overlap = min(xs, ms)
                product_contributions.append({
                    "product": prod,
                    "overlap": round(overlap, 6),
                    "export_share": round(xs, 6),
                    "import_share": round(ms, 6),
                })

            product_contributions.sort(key=lambda x: x["overlap"], reverse=True)

            results_by_partner.append({
                "partner": p,
                "tci": round(tci, 2),
                "n_export_products": len(export_shares),
                "n_import_products": len(import_shares),
                "n_overlapping": len(set(export_shares.keys()) & set(import_shares.keys())),
                "top_complementary_products": product_contributions[:5],
            })

        if not results_by_partner:
            return {"score": None, "signal": "UNAVAILABLE", "error": "could not compute TCI"}

        # Sort by TCI
        results_by_partner.sort(key=lambda x: x["tci"], reverse=True)

        # Score: low average TCI with top partners = high vulnerability
        avg_tci = float(np.mean([r["tci"] for r in results_by_partner]))
        # TCI of 50+ is good, below 30 is weak
        score = max(0.0, min(100.0, (100.0 - avg_tci)))

        return {
            "score": round(score, 2),
            "country": country,
            "average_tci": round(avg_tci, 2),
            "n_partners_analyzed": len(results_by_partner),
            "partner_results": results_by_partner,
        }

    @staticmethod
    def _compute_tci(
        export_shares: dict[str, float], import_shares: dict[str, float]
    ) -> float:
        """Michaely Trade Complementarity Index."""
        all_products = set(export_shares.keys()) | set(import_shares.keys())
        abs_diff_sum = 0.0
        for prod in all_products:
            xs = export_shares.get(prod, 0.0)
            ms = import_shares.get(prod, 0.0)
            abs_diff_sum += abs(ms - xs)
        tci = 100.0 * (1.0 - 0.5 * abs_diff_sum)
        return max(0.0, min(100.0, tci))
