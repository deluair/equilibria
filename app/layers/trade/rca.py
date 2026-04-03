"""Revealed Comparative Advantage (RCA) analysis.

Balassa (1965) RCA index measures a country's export specialization:
    RCA_ik = (X_ik / X_i) / (X_wk / X_w)

where X_ik = country i's exports of product k, X_i = country i's total exports,
X_wk = world exports of product k, X_w = total world exports.

RCA > 1 indicates the country has a revealed comparative advantage in product k
(exports a disproportionately large share relative to world trade).

Also computes:
- RSCA (Revealed Symmetric Comparative Advantage): (RCA-1)/(RCA+1), bounded [-1,1]
- Dynamic RCA: year-over-year changes to identify emerging/declining advantages
- Diversification: number of products with RCA > 1

The score reflects export fragility: heavy reliance on few RCA > 1 products
or declining RCA in key sectors pushes the score higher (more vulnerable).
"""

import numpy as np

from app.layers.base import LayerBase


class RevealedComparativeAdvantage(LayerBase):
    layer_id = "l1"
    name = "Revealed Comparative Advantage"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")
        top_n = kwargs.get("top_n", 20)

        # Fetch export data by product for the country and world
        year_filter = "AND dp.date = ?" if year else ""
        base_params: list = [country]
        if year:
            base_params.append(str(year))

        country_exports = await db.fetch_all(
            f"""
            SELECT ds.series_id AS product_code, dp.date,
                   SUM(dp.value) AS export_value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.country_iso3 = ?
              AND ds.name LIKE '%export%'
              {year_filter}
            GROUP BY ds.series_id, dp.date
            """,
            tuple(base_params),
        )

        if not country_exports:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no export data by product"}

        # Fetch world exports by product
        world_params: list = []
        if year:
            world_params.append(str(year))

        world_exports = await db.fetch_all(
            f"""
            SELECT ds.series_id AS product_code, dp.date,
                   SUM(dp.value) AS export_value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.name LIKE '%export%'
              {year_filter}
            GROUP BY ds.series_id, dp.date
            """,
            tuple(world_params),
        )

        if not world_exports:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no world export data"}

        # Organize by date for dynamic analysis
        dates = sorted({r["date"] for r in country_exports})

        all_rca = {}
        for date in dates:
            # Country exports by product for this date
            c_prods = {
                r["product_code"]: r["export_value"]
                for r in country_exports
                if r["date"] == date and r["export_value"] and r["export_value"] > 0
            }
            # World exports by product for this date
            w_prods = {
                r["product_code"]: r["export_value"]
                for r in world_exports
                if r["date"] == date and r["export_value"] and r["export_value"] > 0
            }

            if not c_prods or not w_prods:
                continue

            total_country = sum(c_prods.values())
            total_world = sum(w_prods.values())

            if total_country <= 0 or total_world <= 0:
                continue

            rca_vals = {}
            for product, x_ik in c_prods.items():
                x_wk = w_prods.get(product, 0)
                if x_wk <= 0:
                    continue
                rca = (x_ik / total_country) / (x_wk / total_world)
                rca_vals[product] = rca

            all_rca[date] = rca_vals

        if not all_rca:
            return {"score": None, "signal": "UNAVAILABLE", "error": "could not compute RCA"}

        # Use latest date for main results
        latest_date = max(all_rca.keys())
        latest_rca = all_rca[latest_date]

        # RSCA: (RCA-1)/(RCA+1)
        rsca = {p: (r - 1) / (r + 1) for p, r in latest_rca.items()}

        # Products with RCA > 1
        advantage_products = {p: r for p, r in latest_rca.items() if r > 1.0}
        n_advantage = len(advantage_products)
        n_total = len(latest_rca)

        # Top products by RCA
        sorted_rca = sorted(latest_rca.items(), key=lambda x: x[1], reverse=True)[:top_n]

        # Dynamic RCA: compare latest to previous period
        dynamic_rca = None
        sorted_dates = sorted(all_rca.keys())
        if len(sorted_dates) >= 2:
            prev_date = sorted_dates[-2]
            prev_rca = all_rca[prev_date]
            dynamic_rca = {}
            for product in latest_rca:
                if product in prev_rca and prev_rca[product] > 0:
                    change = latest_rca[product] - prev_rca[product]
                    pct_change = change / prev_rca[product]
                    dynamic_rca[product] = {
                        "current_rca": round(latest_rca[product], 4),
                        "previous_rca": round(prev_rca[product], 4),
                        "change": round(change, 4),
                        "pct_change": round(pct_change, 4),
                    }

        # Concentration of RCA > 1 products
        if advantage_products:
            adv_values = np.array(list(advantage_products.values()))
            shares = adv_values / adv_values.sum()
            hhi_advantage = float(np.sum(shares ** 2))
        else:
            hhi_advantage = 1.0

        # Score: vulnerability from low diversification + declining RCA
        diversification_ratio = n_advantage / max(n_total, 1)
        # Low diversification = high score, high HHI among advantages = high score
        score = (1.0 - diversification_ratio) * 50.0 + hhi_advantage * 50.0
        score = max(0.0, min(100.0, score))

        result = {
            "score": round(score, 2),
            "country": country,
            "date": latest_date,
            "n_products_total": n_total,
            "n_products_rca_above_1": n_advantage,
            "diversification_ratio": round(diversification_ratio, 4),
            "hhi_advantage_products": round(hhi_advantage, 4),
            "top_rca": [
                {"product": p, "rca": round(r, 4), "rsca": round(rsca.get(p, 0), 4)}
                for p, r in sorted_rca
            ],
        }

        if dynamic_rca:
            # Top emerging (biggest positive change)
            emerging = sorted(dynamic_rca.items(), key=lambda x: x[1]["change"], reverse=True)[:5]
            declining = sorted(dynamic_rca.items(), key=lambda x: x[1]["change"])[:5]
            result["dynamic"] = {
                "emerging": [{"product": p, **v} for p, v in emerging],
                "declining": [{"product": p, **v} for p, v in declining],
                "comparison_dates": [sorted_dates[-2], latest_date],
            }

        return result
