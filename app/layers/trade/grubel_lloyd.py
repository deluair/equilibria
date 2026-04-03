"""Grubel-Lloyd intra-industry trade index.

The Grubel-Lloyd (1975) index measures the extent of intra-industry trade
(IIT), where a country simultaneously exports and imports products within
the same industry classification:

    GL_k = 1 - |X_k - M_k| / (X_k + M_k)

GL ranges from 0 (pure inter-industry trade: country only exports or only
imports product k) to 1 (perfect intra-industry trade: exports = imports).

Aggregate GL index:
    GL = 1 - sum_k |X_k - M_k| / sum_k (X_k + M_k)

The aggregate index is subject to categorical aggregation bias (Grubel &
Lloyd 1975), which the adjusted index corrects for:
    GL_adj = GL / (1 - |X - M| / (X + M))

Marginal IIT (Hamilton & Kniest 1991) measures whether trade growth at the
margin is intra-industry:
    MIIT_k = 1 - |dX_k - dM_k| / (|dX_k| + |dM_k|)

High IIT indicates product differentiation, economies of scale, and similar
factor endowments between trading partners (Helpman-Krugman framework).
Low IIT indicates Heckscher-Ohlin factor-proportion driven trade.

The score reflects structural exposure: low IIT suggests the country's trade
is driven by comparative advantage in few sectors (more vulnerable to shocks).
"""

import numpy as np
from app.layers.base import LayerBase


class GrubelLloyd(LayerBase):
    layer_id = "l1"
    name = "Grubel-Lloyd Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_filter = "AND dp.date = ?" if year else ""

        # Fetch exports and imports by product
        params: list = [country]
        if year:
            params.append(str(year))

        export_rows = await db.fetch_all(
            f"""
            SELECT ds.series_id AS product_code, dp.date,
                   SUM(dp.value) AS value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.country_iso3 = ?
              AND ds.name LIKE '%export%'
              AND dp.value > 0
              {year_filter}
            GROUP BY ds.series_id, dp.date
            """,
            tuple(params),
        )

        import_params: list = [country]
        if year:
            import_params.append(str(year))

        import_rows = await db.fetch_all(
            f"""
            SELECT ds.series_id AS product_code, dp.date,
                   SUM(dp.value) AS value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade')
              AND ds.country_iso3 = ?
              AND ds.name LIKE '%import%'
              AND dp.value > 0
              {year_filter}
            GROUP BY ds.series_id, dp.date
            """,
            tuple(import_params),
        )

        if not export_rows and not import_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no trade data by product"}

        # Organize by date: {date: {product: value}}
        exports_by_date: dict[str, dict[str, float]] = {}
        for r in export_rows:
            exports_by_date.setdefault(r["date"], {})[r["product_code"]] = r["value"]

        imports_by_date: dict[str, dict[str, float]] = {}
        for r in import_rows:
            imports_by_date.setdefault(r["date"], {})[r["product_code"]] = r["value"]

        common_dates = sorted(set(exports_by_date.keys()) & set(imports_by_date.keys()))
        if not common_dates:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping export/import dates"}

        # Compute GL for each date
        gl_series = []
        for d in common_dates:
            x_by_prod = exports_by_date[d]
            m_by_prod = imports_by_date[d]
            all_products = set(x_by_prod.keys()) | set(m_by_prod.keys())

            sum_trade = 0.0
            sum_abs_diff = 0.0
            product_gl = {}

            for prod in all_products:
                xk = x_by_prod.get(prod, 0.0)
                mk = m_by_prod.get(prod, 0.0)
                trade = xk + mk
                if trade <= 0:
                    continue
                gl_k = 1.0 - abs(xk - mk) / trade
                product_gl[prod] = gl_k
                sum_trade += trade
                sum_abs_diff += abs(xk - mk)

            if sum_trade <= 0:
                continue

            # Aggregate GL
            gl_agg = 1.0 - sum_abs_diff / sum_trade

            # Trade imbalance
            total_x = sum(x_by_prod.values())
            total_m = sum(m_by_prod.values())
            imbalance = abs(total_x - total_m) / (total_x + total_m) if (total_x + total_m) > 0 else 0

            # Adjusted GL (corrects for aggregate trade imbalance)
            gl_adj = gl_agg / (1.0 - imbalance) if imbalance < 1.0 else gl_agg

            # Top IIT products (highest GL)
            sorted_gl = sorted(product_gl.items(), key=lambda x: x[1], reverse=True)

            gl_series.append({
                "date": d,
                "gl_aggregate": gl_agg,
                "gl_adjusted": min(1.0, gl_adj),
                "trade_imbalance": imbalance,
                "n_products": len(product_gl),
                "n_iit_products": sum(1 for v in product_gl.values() if v > 0.5),
                "top_iit_products": [
                    {"product": p, "gl": round(v, 4)} for p, v in sorted_gl[:10]
                ],
            })

        if not gl_series:
            return {"score": None, "signal": "UNAVAILABLE", "error": "could not compute GL index"}

        latest = gl_series[-1]

        # Marginal IIT (if multiple periods)
        miit = None
        if len(gl_series) >= 2:
            prev_x = exports_by_date[gl_series[-2]["date"]]
            curr_x = exports_by_date[gl_series[-1]["date"]]
            prev_m = imports_by_date[gl_series[-2]["date"]]
            curr_m = imports_by_date[gl_series[-1]["date"]]

            all_p = set(prev_x.keys()) | set(curr_x.keys()) | set(prev_m.keys()) | set(curr_m.keys())
            sum_abs_d = 0.0
            sum_abs_dx_dm = 0.0

            for p in all_p:
                dx = curr_x.get(p, 0) - prev_x.get(p, 0)
                dm = curr_m.get(p, 0) - prev_m.get(p, 0)
                sum_abs_d += abs(dx - dm)
                sum_abs_dx_dm += abs(dx) + abs(dm)

            if sum_abs_dx_dm > 0:
                miit_val = 1.0 - sum_abs_d / sum_abs_dx_dm
                miit = {
                    "value": round(miit_val, 4),
                    "period": [gl_series[-2]["date"], gl_series[-1]["date"]],
                    "interpretation": (
                        "Trade growth is predominantly intra-industry"
                        if miit_val > 0.5
                        else "Trade growth is predominantly inter-industry"
                    ),
                }

        # Trend
        gl_vals = np.array([g["gl_aggregate"] for g in gl_series])
        trend = "stable"
        if len(gl_vals) >= 3:
            t = np.arange(len(gl_vals), dtype=float)
            slope = np.polyfit(t, gl_vals, 1)[0]
            if slope > 0.005:
                trend = "increasing IIT"
            elif slope < -0.005:
                trend = "decreasing IIT"

        # Score: low IIT = high inter-industry specialization = more exposed
        score = max(0.0, min(100.0, (1.0 - latest["gl_aggregate"]) * 100.0))

        result = {
            "score": round(score, 2),
            "country": country,
            "latest": {
                "date": latest["date"],
                "gl_aggregate": round(latest["gl_aggregate"], 4),
                "gl_adjusted": round(latest["gl_adjusted"], 4),
                "trade_imbalance": round(latest["trade_imbalance"], 4),
                "n_products": latest["n_products"],
                "n_iit_products": latest["n_iit_products"],
                "share_iit_products": round(
                    latest["n_iit_products"] / max(latest["n_products"], 1), 4
                ),
            },
            "trend": trend,
            "n_periods": len(gl_series),
            "interpretation": (
                "High intra-industry trade: differentiated products, scale economies"
                if latest["gl_aggregate"] > 0.5
                else "Low intra-industry trade: comparative advantage driven, inter-industry pattern"
            ),
        }

        if miit:
            result["marginal_iit"] = miit

        return result
