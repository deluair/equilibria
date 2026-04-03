"""Bilateral trade cost measure derived from gravity (Novy 2013).

Methodology:
    Compute comprehensive bilateral trade costs from observed trade flows
    using the micro-founded measure of Novy (2013). The key insight is
    that bilateral trade costs can be inferred from the ratio of
    international to intranational trade.

    Trade cost measure:
        tau_ij = (t_ii * t_jj / t_ij * t_ji)^{1/(2*(sigma-1))} - 1

    where:
        t_ij = (x_ij / (x_ii * x_jj))^{1/(1-sigma)} is the bilateral
        trade resistance, x_ij is bilateral trade, x_ii is intranational
        trade (GDP - total exports), and sigma is the elasticity of
        substitution.

    Decomposition into components:
    1. Tariff component (ad valorem equivalent)
    2. Transport costs (distance-based proxy)
    3. Border/non-tariff barriers (residual)

    Score (0-100): Higher score means higher trade costs relative to peers.

References:
    Novy, D. (2013). "Gravity Redux: Measuring International Trade Costs
        with Panel Data." Economic Inquiry, 51(1), 101-121.
    Anderson, J.E. and van Wincoop, E. (2004). "Trade Costs." Journal
        of Economic Literature, 42(3), 691-751.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TradeCost(LayerBase):
    layer_id = "l1"
    name = "Trade Cost (Novy)"

    async def compute(self, db, **kwargs) -> dict:
        """Compute Novy bilateral trade costs and decomposition.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 country code
            partner : str - ISO3 partner (optional, computes for all if omitted)
            year : int - reference year
            sigma : float - elasticity of substitution (default 5.0)
        """
        reporter = kwargs.get("reporter", "USA")
        partner = kwargs.get("partner")
        year = kwargs.get("year", 2022)
        sigma = kwargs.get("sigma", 5.0)

        # Fetch bilateral trade + GDP for Novy measure
        if partner:
            query = """
                SELECT partner_iso3, trade_value as x_ij, reverse_trade as x_ji,
                       gdp_reporter, gdp_partner, total_exports_reporter,
                       total_exports_partner, distance, tariff_rate
                FROM bilateral_trade
                WHERE reporter_iso3 = ? AND partner_iso3 = ? AND year = ?
            """
            params = (reporter, partner, year)
        else:
            query = """
                SELECT partner_iso3, trade_value as x_ij, reverse_trade as x_ji,
                       gdp_reporter, gdp_partner, total_exports_reporter,
                       total_exports_partner, distance, tariff_rate
                FROM bilateral_trade
                WHERE reporter_iso3 = ? AND year = ?
            """
            params = (reporter, year)

        rows = await db.execute(query, params)
        records = await rows.fetchall()

        if not records:
            return {"score": 50.0, "avg_trade_cost": None,
                    "note": "No bilateral trade data available"}

        results = []
        trade_costs = []

        for r in records:
            x_ij = float(r["x_ij"] or 0)
            x_ji = float(r["x_ji"] or 0)
            gdp_i = float(r["gdp_reporter"] or 0)
            gdp_j = float(r["gdp_partner"] or 0)
            total_exp_i = float(r["total_exports_reporter"] or 0)
            total_exp_j = float(r["total_exports_partner"] or 0)
            dist = float(r["distance"] or 1)
            tariff = float(r["tariff_rate"] or 0)

            # Intranational trade: GDP - total exports (proxy for domestic absorption)
            x_ii = max(gdp_i - total_exp_i, 1.0)
            x_jj = max(gdp_j - total_exp_j, 1.0)

            if x_ij <= 0 or x_ji <= 0:
                continue

            # Novy trade cost measure
            # tau_ij = ((x_ii * x_jj) / (x_ij * x_ji))^{1/(2*(sigma-1))} - 1
            exponent = 1.0 / (2.0 * (sigma - 1.0))
            ratio = (x_ii * x_jj) / (x_ij * x_ji)

            if ratio <= 0:
                continue

            tau = ratio ** exponent - 1.0
            tau_pct = tau * 100  # as ad valorem equivalent percentage

            # Decomposition (simplified)
            # Tariff component: direct tariff rate
            tariff_cost = tariff

            # Transport cost proxy: distance-based (rough calibration)
            # Anderson & van Wincoop (2004): ~1.7% per 1000km
            transport_cost = dist / 1000.0 * 1.7

            # Border/NTB cost: residual
            border_cost = max(tau_pct - tariff_cost - transport_cost, 0)

            trade_costs.append(tau_pct)
            results.append({
                "partner": r["partner_iso3"],
                "trade_cost_pct": float(tau_pct),
                "tariff_component": float(tariff_cost),
                "transport_component": float(transport_cost),
                "border_component": float(border_cost),
                "bilateral_trade": float(x_ij),
                "reverse_trade": float(x_ji),
                "distance": float(dist),
            })

        if not trade_costs:
            return {"score": 50.0, "avg_trade_cost": None,
                    "note": "Could not compute trade costs (zero flows)"}

        tc_arr = np.array(trade_costs)
        avg_cost = float(np.mean(tc_arr))
        median_cost = float(np.median(tc_arr))
        std_cost = float(np.std(tc_arr))

        # Trade-weighted average cost
        trade_vals = np.array([r["bilateral_trade"] for r in results])
        total_trade = trade_vals.sum()
        if total_trade > 0:
            weighted_avg = float(np.average(tc_arr, weights=trade_vals))
        else:
            weighted_avg = avg_cost

        # Average decomposition
        avg_tariff = float(np.mean([r["tariff_component"] for r in results]))
        avg_transport = float(np.mean([r["transport_component"] for r in results]))
        avg_border = float(np.mean([r["border_component"] for r in results]))

        # Sort by trade cost
        results.sort(key=lambda x: x["trade_cost_pct"])
        lowest_cost = results[:5]
        highest_cost = results[-5:][::-1]

        # Score: higher trade costs = higher score
        # Benchmark: 50% trade cost is median globally
        score = float(np.clip(weighted_avg / 150 * 100, 0, 100))

        return {
            "score": score,
            "avg_trade_cost": avg_cost,
            "median_trade_cost": median_cost,
            "weighted_avg_trade_cost": weighted_avg,
            "std_trade_cost": std_cost,
            "avg_tariff_component": avg_tariff,
            "avg_transport_component": avg_transport,
            "avg_border_component": avg_border,
            "n_partners": len(results),
            "lowest_cost_partners": lowest_cost,
            "highest_cost_partners": highest_cost,
            "sigma": sigma,
            "reporter": reporter,
            "year": year,
        }
