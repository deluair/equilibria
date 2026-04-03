"""Bilateral trade decomposition into extensive and intensive margins.

Following Hummels & Klenow (2005), bilateral trade growth can be decomposed:
    X_ij = EM_ij * IM_ij

Extensive margin (EM): the number of distinct product categories traded.
Measures trade breadth.
    EM_ij = sum_{k in K_ij} (X_wk / X_w)

where K_ij is the set of products exported by i to j, and the weights are
world export shares.

Intensive margin (IM): the average value per traded product.  Measures
trade depth within existing product lines.
    IM_ij = X_ij / (sum_{k in K_ij} X_wk)

Trade growth can come from:
- New products being traded (extensive margin expansion)
- More value in existing products (intensive margin deepening)

This decomposition helps diagnose whether a country's trade growth reflects
genuine diversification or simply more of the same products.

The score reflects fragility: narrow extensive margin (few products) or
unstable intensive margin (volatile value per product) signals vulnerability.
"""

import numpy as np
from app.layers.base import LayerBase


class BilateralDecomposition(LayerBase):
    layer_id = "l1"
    name = "Bilateral Decomposition"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        partner = kwargs.get("partner_iso3")
        year = kwargs.get("year")

        filters = ["ds.source IN ('baci', 'comtrade')", "ds.country_iso3 = ?"]
        params: list = [country]

        if partner:
            filters.append("ds.description LIKE '%' || ? || '%'")
            params.append(partner)

        if year:
            filters.append("dp.date = ?")
            params.append(str(year))

        where = " AND ".join(filters)

        # Country's bilateral exports by product
        bilateral = await db.fetch_all(
            f"""
            SELECT ds.series_id AS product_code, ds.description AS partner_info,
                   dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE {where}
              AND ds.name LIKE '%export%'
              AND dp.value > 0
            ORDER BY dp.date
            """,
            tuple(params),
        )

        if not bilateral:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no bilateral export data"}

        # World exports by product (for weighting)
        world_params: list = []
        world_year_filter = "AND dp.date = ?" if year else ""
        if year:
            world_params.append(str(year))

        world_exports = await db.fetch_all(
            f"""
            SELECT ds.series_id AS product_code, dp.date,
                   SUM(dp.value) AS world_value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.name LIKE '%export%'
              AND dp.value > 0
              {world_year_filter}
            GROUP BY ds.series_id, dp.date
            """,
            tuple(world_params),
        )

        # Organize world exports: {date: {product: value}}
        world_by_date: dict[str, dict[str, float]] = {}
        for row in world_exports:
            d = row["date"]
            world_by_date.setdefault(d, {})[row["product_code"]] = row["world_value"]

        # Total world exports per date
        world_totals: dict[str, float] = {}
        for d, prods in world_by_date.items():
            world_totals[d] = sum(prods.values())

        # Organize bilateral: {date: {product: value}}
        bilateral_by_date: dict[str, dict[str, float]] = {}
        for row in bilateral:
            d = row["date"]
            bilateral_by_date.setdefault(d, {})[row["product_code"]] = row["value"]

        # Compute margins for each date
        dates = sorted(set(bilateral_by_date.keys()) & set(world_by_date.keys()))
        if not dates:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping dates"}

        margins = []
        for d in dates:
            b_prods = bilateral_by_date[d]
            w_prods = world_by_date.get(d, {})
            w_total = world_totals.get(d, 0)

            if w_total <= 0:
                continue

            # Products traded bilaterally that also exist in world data
            traded = set(b_prods.keys()) & set(w_prods.keys())
            if not traded:
                continue

            # Extensive margin: share of world trade covered by traded products
            em = sum(w_prods[k] for k in traded) / w_total

            # Intensive margin: bilateral trade / world trade in those products
            world_traded_sum = sum(w_prods[k] for k in traded)
            bilateral_sum = sum(b_prods[k] for k in traded)
            im = bilateral_sum / world_traded_sum if world_traded_sum > 0 else 0.0

            # Number of products (simple count)
            n_products = len(traded)

            # Average value per product
            avg_value = bilateral_sum / n_products if n_products > 0 else 0.0

            margins.append({
                "date": d,
                "extensive_margin": em,
                "intensive_margin": im,
                "n_products": n_products,
                "total_value": bilateral_sum,
                "avg_value_per_product": avg_value,
            })

        if not margins:
            return {"score": None, "signal": "UNAVAILABLE", "error": "could not compute margins"}

        latest = margins[-1]

        # Trend analysis if multiple periods
        growth_decomposition = None
        if len(margins) >= 2:
            em_vals = np.array([m["extensive_margin"] for m in margins])
            im_vals = np.array([m["intensive_margin"] for m in margins])

            if em_vals[0] > 0 and im_vals[0] > 0:
                em_growth = (em_vals[-1] / em_vals[0]) - 1.0
                im_growth = (im_vals[-1] / im_vals[0]) - 1.0
                total_growth = ((em_vals[-1] * im_vals[-1]) / (em_vals[0] * im_vals[0])) - 1.0

                growth_decomposition = {
                    "extensive_margin_growth": round(em_growth, 4),
                    "intensive_margin_growth": round(im_growth, 4),
                    "total_growth": round(total_growth, 4),
                    "extensive_share_of_growth": (
                        round(em_growth / total_growth, 4) if abs(total_growth) > 1e-10 else None
                    ),
                    "period": [margins[0]["date"], margins[-1]["date"]],
                }

        # Score: narrow extensive margin + volatile intensive margin = vulnerable
        em_score = max(0.0, (1.0 - latest["extensive_margin"]) * 60.0)
        # Volatility of intensive margin
        if len(margins) >= 3:
            im_vals = np.array([m["intensive_margin"] for m in margins])
            im_vol = float(np.std(np.diff(im_vals) / np.maximum(im_vals[:-1], 1e-10)))
            vol_score = min(40.0, im_vol * 200.0)
        else:
            vol_score = 20.0

        score = max(0.0, min(100.0, em_score + vol_score))

        result = {
            "score": round(score, 2),
            "country": country,
            "latest": {
                "date": latest["date"],
                "extensive_margin": round(latest["extensive_margin"], 4),
                "intensive_margin": round(latest["intensive_margin"], 6),
                "n_products": latest["n_products"],
                "total_value": round(latest["total_value"], 2),
                "avg_value_per_product": round(latest["avg_value_per_product"], 2),
            },
            "n_periods": len(margins),
        }

        if partner:
            result["partner"] = partner
        if growth_decomposition:
            result["growth_decomposition"] = growth_decomposition

        return result
