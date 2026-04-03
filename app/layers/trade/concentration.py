"""Trade concentration: HHI, Theil index, and diversification scoring.

Herfindahl-Hirschman Index (HHI) for trade:
    HHI = sum(s_k^2)

where s_k is the share of product k in total exports (or imports).
HHI ranges from 1/N (perfectly diversified) to 1.0 (single product).

Theil Entropy Index (more sensitive to the tails of the distribution):
    T = sum(s_k * ln(N * s_k))

where N is the number of products.  T = 0 for perfect diversification,
T = ln(N) for maximum concentration.

Market concentration mirrors the product dimension but across destinations:
how concentrated are exports across partner countries?

The score reflects export vulnerability: high product or market
concentration means a shock to one sector or partner can devastate
trade revenue.  Score is the average of product and market HHI
mapped to 0-100.
"""

import numpy as np

from app.layers.base import LayerBase


class TradeConcentration(LayerBase):
    layer_id = "l1"
    name = "Trade Concentration"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")
        side = kwargs.get("side", "export")  # "export" or "import"

        year_filter = "AND dp.date = ?" if year else ""
        params: list = [country, f"%{side}%"]
        if year:
            params.append(str(year))

        # Product concentration
        product_rows = await db.fetch_all(
            f"""
            SELECT ds.series_id AS product_code, SUM(dp.value) AS value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.country_iso3 = ?
              AND ds.name LIKE ?
              {year_filter}
            GROUP BY ds.series_id
            HAVING value > 0
            """,
            tuple(params),
        )

        # Market (partner) concentration
        market_params: list = [country, f"%{side}%"]
        if year:
            market_params.append(str(year))

        market_rows = await db.fetch_all(
            f"""
            SELECT ds.description AS partner, SUM(dp.value) AS value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.country_iso3 = ?
              AND ds.name LIKE ?
              {year_filter}
              AND ds.description IS NOT NULL
            GROUP BY ds.description
            HAVING value > 0
            """,
            tuple(market_params),
        )

        product_result = self._compute_concentration(
            [r["value"] for r in product_rows] if product_rows else []
        )
        market_result = self._compute_concentration(
            [r["value"] for r in market_rows] if market_rows else []
        )

        if product_result is None and market_result is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no trade data for concentration"}

        # Top products and partners
        top_products = []
        if product_rows:
            total = sum(r["value"] for r in product_rows)
            sorted_prods = sorted(product_rows, key=lambda r: r["value"], reverse=True)[:10]
            for r in sorted_prods:
                top_products.append({
                    "product": r["product_code"],
                    "value": round(r["value"], 2),
                    "share": round(r["value"] / total, 4) if total > 0 else 0,
                })

        top_markets = []
        if market_rows:
            total = sum(r["value"] for r in market_rows)
            sorted_mkts = sorted(market_rows, key=lambda r: r["value"], reverse=True)[:10]
            for r in sorted_mkts:
                top_markets.append({
                    "partner": r["partner"],
                    "value": round(r["value"], 2),
                    "share": round(r["value"] / total, 4) if total > 0 else 0,
                })

        # Score: average of product and market HHI, mapped to 0-100
        scores = []
        if product_result:
            # HHI of 0.25+ is highly concentrated (DOJ threshold)
            scores.append(min(100.0, product_result["hhi"] * 200.0))
        if market_result:
            scores.append(min(100.0, market_result["hhi"] * 200.0))
        score = float(np.mean(scores)) if scores else 50.0

        result = {
            "score": round(score, 2),
            "country": country,
            "side": side,
        }

        if product_result:
            result["product_concentration"] = {
                "hhi": product_result["hhi"],
                "normalized_hhi": product_result["normalized_hhi"],
                "theil": product_result["theil"],
                "n_products": product_result["n"],
                "classification": self._classify_hhi(product_result["hhi"]),
            }
        if market_result:
            result["market_concentration"] = {
                "hhi": market_result["hhi"],
                "normalized_hhi": market_result["normalized_hhi"],
                "theil": market_result["theil"],
                "n_partners": market_result["n"],
                "classification": self._classify_hhi(market_result["hhi"]),
            }

        if top_products:
            result["top_products"] = top_products
        if top_markets:
            result["top_markets"] = top_markets

        return result

    @staticmethod
    def _compute_concentration(values: list[float]) -> dict | None:
        if not values or len(values) < 2:
            return None

        arr = np.array(values, dtype=float)
        total = arr.sum()
        if total <= 0:
            return None

        shares = arr / total
        n = len(shares)

        # HHI
        hhi = float(np.sum(shares ** 2))

        # Normalized HHI: (HHI - 1/N) / (1 - 1/N)
        if n > 1:
            nhhi = (hhi - 1.0 / n) / (1.0 - 1.0 / n)
        else:
            nhhi = 1.0

        # Theil entropy index
        # T = sum(s_k * ln(N * s_k)) for s_k > 0
        mask = shares > 0
        theil = float(np.sum(shares[mask] * np.log(n * shares[mask])))

        return {
            "hhi": round(hhi, 6),
            "normalized_hhi": round(max(0.0, nhhi), 6),
            "theil": round(theil, 6),
            "n": int(n),
        }

    @staticmethod
    def _classify_hhi(hhi: float) -> str:
        """US DOJ/FTC merger guideline thresholds adapted for trade."""
        if hhi < 0.10:
            return "diversified"
        elif hhi < 0.18:
            return "moderately concentrated"
        elif hhi < 0.25:
            return "concentrated"
        else:
            return "highly concentrated"
