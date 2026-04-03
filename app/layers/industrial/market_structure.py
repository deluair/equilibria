"""Market structure analysis: concentration, entry barriers, and market power.

The Herfindahl-Hirschman Index (HHI) is the standard measure of market
concentration, computed as the sum of squared market shares:

    HHI = sum_i (s_i^2)

where s_i is firm i's market share (0-1 scale). HHI ranges from 1/N
(perfect competition) to 1 (monopoly). DOJ/FTC thresholds:
    HHI < 0.15: unconcentrated
    0.15 <= HHI < 0.25: moderately concentrated
    HHI >= 0.25: highly concentrated

CR4/CR8 concentration ratios measure the combined market share of the top
4 or 8 firms. CR4 > 0.60 or CR8 > 0.80 typically indicates oligopoly.

The Lerner index measures market power as the markup over marginal cost:

    L = (P - MC) / P = 1 / |epsilon|

where epsilon is the firm's own-price elasticity of demand. L = 0 under
perfect competition, approaching 1 under monopoly.

Entry barriers are measured via:
    - Minimum efficient scale (MES) as share of market output
    - Sunk cost ratio (irrecoverable entry investment / total investment)
    - Incumbent advertising-to-sales ratio (brand loyalty barrier)

References:
    Tirole, J. (1988). The Theory of Industrial Organization. MIT Press.
    Bain, J. (1956). Barriers to New Competition. Harvard University Press.
    DOJ/FTC (2023). Merger Guidelines, Section 2.

Score: high HHI -> STRESS/CRISIS (monopoly power), low HHI -> STABLE.
"""

import json

import numpy as np

from app.layers.base import LayerBase


class MarketStructure(LayerBase):
    layer_id = "l14"
    name = "Market Structure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        sector = kwargs.get("sector")
        year = kwargs.get("year")

        clauses = ["ds.country_iso3 = ?", "ds.source = ?"]
        params: list = [country, "market_structure"]
        if sector:
            clauses.append("ds.description LIKE ?")
            params.append(f"%{sector}%")
        if year:
            clauses.append("dp.date = ?")
            params.append(str(year))

        where = " AND ".join(clauses)
        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE {where}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient market data"}

        shares = []
        prices = []
        marginal_costs = []
        entry_data = {"mes_share": None, "sunk_ratio": None, "ad_sales_ratio": None}

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            share = meta.get("market_share")
            if share is not None:
                shares.append(float(share))
            price = meta.get("price")
            mc = meta.get("marginal_cost")
            if price is not None and mc is not None:
                prices.append(float(price))
                marginal_costs.append(float(mc))
            for key in ("mes_share", "sunk_ratio", "ad_sales_ratio"):
                if meta.get(key) is not None:
                    entry_data[key] = float(meta[key])

        if len(shares) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient firm shares"}

        shares_arr = np.array(sorted(shares, reverse=True))
        shares_arr = shares_arr / shares_arr.sum() if shares_arr.sum() > 0 else shares_arr

        # HHI
        hhi = float(np.sum(shares_arr ** 2))

        # CR4, CR8
        cr4 = float(np.sum(shares_arr[:4])) if len(shares_arr) >= 4 else float(np.sum(shares_arr))
        cr8 = float(np.sum(shares_arr[:8])) if len(shares_arr) >= 8 else float(np.sum(shares_arr))

        # Lerner index
        lerner = None
        if prices and marginal_costs:
            p_arr = np.array(prices)
            mc_arr = np.array(marginal_costs)
            valid = p_arr > 0
            if valid.any():
                markups = (p_arr[valid] - mc_arr[valid]) / p_arr[valid]
                lerner = float(np.mean(markups))

        # Number-equivalent firms (1/HHI)
        n_equivalent = 1.0 / hhi if hhi > 0 else len(shares_arr)

        # Entry barriers composite (0-1 scale)
        barrier_components = []
        if entry_data["mes_share"] is not None:
            barrier_components.append(min(entry_data["mes_share"] / 0.20, 1.0))
        if entry_data["sunk_ratio"] is not None:
            barrier_components.append(entry_data["sunk_ratio"])
        if entry_data["ad_sales_ratio"] is not None:
            barrier_components.append(min(entry_data["ad_sales_ratio"] / 0.10, 1.0))
        entry_barrier_index = float(np.mean(barrier_components)) if barrier_components else None

        # DOJ/FTC classification
        if hhi < 0.15:
            concentration_class = "unconcentrated"
        elif hhi < 0.25:
            concentration_class = "moderately concentrated"
        else:
            concentration_class = "highly concentrated"

        # Score: HHI maps to concern level
        # HHI 0-0.15 -> STABLE (0-25), 0.15-0.25 -> WATCH (25-50),
        # 0.25-0.50 -> STRESS (50-75), >0.50 -> CRISIS (75-100)
        if hhi < 0.15:
            score = hhi / 0.15 * 25.0
        elif hhi < 0.25:
            score = 25.0 + (hhi - 0.15) / 0.10 * 25.0
        elif hhi < 0.50:
            score = 50.0 + (hhi - 0.25) / 0.25 * 25.0
        else:
            score = 75.0 + min((hhi - 0.50) / 0.50 * 25.0, 25.0)
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_firms": len(shares_arr),
            "hhi": round(hhi, 4),
            "concentration_class": concentration_class,
            "cr4": round(cr4, 4),
            "cr8": round(cr8, 4),
            "n_equivalent_firms": round(n_equivalent, 1),
            "lerner_index": round(lerner, 4) if lerner is not None else None,
            "entry_barriers": {
                "composite_index": round(entry_barrier_index, 4) if entry_barrier_index is not None else None,
                "mes_share": entry_data["mes_share"],
                "sunk_ratio": entry_data["sunk_ratio"],
                "ad_sales_ratio": entry_data["ad_sales_ratio"],
            },
            "share_distribution": {
                "top_firm": round(float(shares_arr[0]), 4),
                "gini": round(self._gini(shares_arr), 4),
            },
        }

    @staticmethod
    def _gini(shares: np.ndarray) -> float:
        """Gini coefficient of market share distribution."""
        n = len(shares)
        if n < 2:
            return 0.0
        sorted_s = np.sort(shares)
        index = np.arange(1, n + 1)
        return float((2.0 * np.sum(index * sorted_s) - (n + 1) * np.sum(sorted_s)) / (n * np.sum(sorted_s)))
