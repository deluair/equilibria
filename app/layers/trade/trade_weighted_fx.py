"""Trade-weighted exchange rate computation.

Methodology:
    Compute nominal and real effective exchange rates (NEER/REER) using
    bilateral trade weights. Following the BIS methodology:

    1. NEER: geometric weighted average of bilateral nominal exchange rates,
       NEER = prod(e_ij ^ w_j) where e_ij is the bilateral exchange rate
       (domestic per foreign) and w_j is the trade weight of partner j.

    2. REER: NEER adjusted for relative price levels,
       REER = NEER * (P_domestic / P_weighted_foreign).

    3. Misalignment: deviation of REER from its long-run equilibrium
       estimated via HP filter or BEER (Behavioral Equilibrium Exchange Rate).

    4. Trade-weighted depreciation/appreciation over specified horizons.

    Score (0-100): Higher score indicates greater REER misalignment
    (overvaluation penalized more than undervaluation for vulnerability).

References:
    Klau, M. and Fung, S.S. (2006). "The new BIS effective exchange rate
        indices." BIS Quarterly Review, March 2006.
    Clark, P.B. and MacDonald, R. (1998). "Exchange rates and economic
        fundamentals: a methodological comparison of BEERs and FEERs."
        IMF Working Paper 98/67.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TradeWeightedFX(LayerBase):
    layer_id = "l1"
    name = "Trade-Weighted Exchange Rate"

    async def compute(self, db, **kwargs) -> dict:
        """Compute NEER, REER, and exchange rate misalignment.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 country code
            year : int - reference year
            base_year : int - base year for index (default 2010)
        """
        reporter = kwargs.get("reporter", "USA")
        year = kwargs.get("year", 2022)
        base_year = kwargs.get("base_year", 2010)

        # Fetch trade weights (share of bilateral trade with each partner)
        rows = await db.execute(
            """
            SELECT partner_iso3,
                   SUM(trade_value) as bilateral_trade
            FROM bilateral_trade
            WHERE reporter_iso3 = ? AND year = ?
            GROUP BY partner_iso3
            HAVING bilateral_trade > 0
            ORDER BY bilateral_trade DESC
            """,
            (reporter, year),
        )
        records = await rows.fetchall()

        if not records:
            return {"score": 50.0, "neer": None, "reer": None,
                    "note": "No trade data for weight computation"}

        partners = [r["partner_iso3"] for r in records]
        trade_vals = np.array([float(r["bilateral_trade"]) for r in records])
        weights = trade_vals / trade_vals.sum()

        # Fetch exchange rates and CPI for reporter and partners
        fx_rows = await db.execute(
            """
            SELECT iso3, year, exchange_rate, cpi
            FROM macro_indicators
            WHERE iso3 IN ({}) AND year BETWEEN ? AND ?
            """.format(",".join(["?"] * (len(partners) + 1))),
            (*partners, reporter, base_year, year),
        )
        fx_records = await fx_rows.fetchall()

        if not fx_records:
            return {"score": 50.0, "neer": None, "reer": None,
                    "note": "No exchange rate data available"}

        # Organize by country and year
        fx_data: dict[str, dict[int, dict]] = {}
        for r in fx_records:
            iso = r["iso3"]
            yr = int(r["year"])
            fx_data.setdefault(iso, {})[yr] = {
                "exchange_rate": float(r["exchange_rate"]),
                "cpi": float(r["cpi"]),
            }

        # Compute NEER for each year in range
        neer_series = {}
        reer_series = {}
        reporter_data = fx_data.get(reporter, {})

        for yr in range(base_year, year + 1):
            if yr not in reporter_data:
                continue

            r_fx = reporter_data[yr]["exchange_rate"]
            r_cpi = reporter_data[yr]["cpi"]

            # Geometric weighted average of bilateral rates
            log_neer = 0.0
            log_reer = 0.0
            valid_weight_sum = 0.0

            for i, p in enumerate(partners):
                p_data = fx_data.get(p, {}).get(yr)
                if p_data is None:
                    continue

                p_fx = p_data["exchange_rate"]
                p_cpi = p_data["cpi"]

                if p_fx > 0 and r_fx > 0:
                    bilateral_rate = r_fx / p_fx
                    log_neer += weights[i] * np.log(bilateral_rate)

                    if p_cpi > 0 and r_cpi > 0:
                        real_rate = bilateral_rate * (r_cpi / p_cpi)
                        log_reer += weights[i] * np.log(real_rate)

                    valid_weight_sum += weights[i]

            if valid_weight_sum > 0:
                neer_series[yr] = float(np.exp(log_neer / valid_weight_sum))
                reer_series[yr] = float(np.exp(log_reer / valid_weight_sum))

        if not neer_series:
            return {"score": 50.0, "neer": None, "reer": None,
                    "note": "Insufficient data for NEER computation"}

        # Normalize to base year = 100
        base_neer = neer_series.get(base_year, 1.0)
        base_reer = reer_series.get(base_year, 1.0)

        neer_index = {yr: v / base_neer * 100 for yr, v in neer_series.items()}
        reer_index = {yr: v / base_reer * 100 for yr, v in reer_series.items()}

        current_neer = neer_index.get(year, 100.0)
        current_reer = reer_index.get(year, 100.0)

        # Misalignment: HP filter on REER to get trend
        reer_vals = np.array([reer_index[yr] for yr in sorted(reer_index.keys())])
        if len(reer_vals) >= 5:
            trend = _hp_filter(reer_vals, lamb=6.25)  # annual lambda
            equilibrium_reer = float(trend[-1])
            misalignment_pct = (current_reer - equilibrium_reer) / equilibrium_reer * 100
        else:
            equilibrium_reer = float(np.mean(reer_vals))
            misalignment_pct = (current_reer - equilibrium_reer) / equilibrium_reer * 100

        # Year-over-year change
        prev_year_neer = neer_index.get(year - 1)
        neer_yoy = ((current_neer / prev_year_neer - 1) * 100
                     if prev_year_neer else None)

        prev_year_reer = reer_index.get(year - 1)
        reer_yoy = ((current_reer / prev_year_reer - 1) * 100
                     if prev_year_reer else None)

        # Score: larger misalignment = higher score
        # Overvaluation (positive misalignment) penalized more
        if misalignment_pct > 0:
            score = float(np.clip(misalignment_pct * 3, 0, 100))
        else:
            score = float(np.clip(abs(misalignment_pct) * 2, 0, 100))

        return {
            "score": score,
            "neer": current_neer,
            "reer": current_reer,
            "neer_yoy_pct": neer_yoy,
            "reer_yoy_pct": reer_yoy,
            "equilibrium_reer": equilibrium_reer,
            "misalignment_pct": float(misalignment_pct),
            "n_partners": len(partners),
            "top_weights": [
                {"partner": partners[i], "weight": float(weights[i])}
                for i in range(min(10, len(partners)))
            ],
            "neer_index": neer_index,
            "reer_index": reer_index,
            "base_year": base_year,
            "reporter": reporter,
            "year": year,
        }


def _hp_filter(y: np.ndarray, lamb: float = 6.25) -> np.ndarray:
    """Hodrick-Prescott filter for trend extraction.

    Parameters
    ----------
    y : 1-d array of time series values
    lamb : smoothing parameter (6.25 for annual data per Ravn-Uhlig)

    Returns
    -------
    trend component
    """
    n = len(y)
    if n < 3:
        return y.copy()

    # Build the second-difference matrix D
    diag_main = np.ones(n) + lamb * 2
    diag_main[0] = 1 + lamb
    diag_main[1] = 1 + lamb * 5
    diag_main[-2] = 1 + lamb * 5
    diag_main[-1] = 1 + lamb

    # Solve (I + lambda * D'D) * tau = y via tridiagonal system
    # Simplified: use direct matrix construction
    eye = np.eye(n)
    D = np.zeros((n - 2, n))
    for i in range(n - 2):
        D[i, i] = 1
        D[i, i + 1] = -2
        D[i, i + 2] = 1

    A = eye + lamb * (D.T @ D)
    try:
        trend = np.linalg.solve(A, y)
    except np.linalg.LinAlgError:
        trend = y.copy()

    return trend
