"""Spatial market integration analysis.

Tests the Law of One Price (LOP) and measures the degree of market
integration across spatially separated agricultural markets. Well-integrated
markets transmit price signals efficiently, ensuring resource allocation
responds to scarcity signals. Poor integration implies market failures,
high transaction costs, or trade barriers.

Components:
    1. **Law of One Price testing**: In perfectly integrated markets,
       price differentials should not exceed transaction costs:
       |P_i - P_j| <= T_ij
       Test via cointegration of price pairs and half-life of deviations.

    2. **Spatial price analysis**: Variance ratio test comparing the
       variance of price levels across markets to variance within markets.
       Higher ratio = less integrated.

    3. **Market integration index** (Ravallion 1986):
       P_i,t = a + b1*P_i,{t-1} + b2*P_ref,t + b3*P_ref,{t-1} + e
       Short-run integration: b2
       Long-run integration: (b2 + b3) / (1 - b1)
       Perfect integration if long-run coefficient = 1.

    4. **Transaction cost bands** (Barrett & Li 2002):
       Parity bound model (PBM) that decomposes price differentials into
       regimes: (a) integrated/efficient, (b) segmented, (c) imperfect
       integration with rents.

Score (0-100): Higher score indicates poorly integrated markets,
suggesting high transaction costs, trade barriers, or market failures.

References:
    Ravallion, M. (1986). "Testing market integration." American Journal
        of Agricultural Economics, 68(1), 102-109.
    Barrett, C.B., Li, J.R. (2002). "Distinguishing between equilibrium
        and integration in spatial price analysis." American Journal of
        Agricultural Economics, 84(2), 292-307.
    Fackler, P.L., Goodwin, B.K. (2001). "Spatial price analysis."
        Handbook of Agricultural Economics, Vol. 1B.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MarketIntegration(LayerBase):
    layer_id = "l5"
    name = "Market Integration"

    async def compute(self, db, **kwargs) -> dict:
        """Compute market integration indicators.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code
            commodity : str - commodity to analyze (default 'rice')
            reference_market : str - reference market ID (largest/capital)
        """
        country = kwargs.get("country_iso3", "BGD")
        commodity = kwargs.get("commodity", "rice")
        ref_market = kwargs.get("reference_market")

        # Fetch regional price series for the commodity
        market_rows = await db.fetch_all(
            """
            SELECT ds.name, ds.description, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.name LIKE ?
              AND ds.source IN ('national', 'fao', 'wfp')
            ORDER BY ds.name, dp.date ASC
            """,
            (country, f"%{commodity}%market%price%"),
        )

        # Group by market
        markets = {}
        for row in market_rows:
            market_name = row["name"]
            if market_name not in markets:
                markets[market_name] = {"dates": [], "values": []}
            markets[market_name]["dates"].append(row["date"])
            markets[market_name]["values"].append(row["value"])

        # If not enough market-level data, try fetching domestic vs international
        if len(markets) < 2:
            # Fallback: domestic vs world price as two "markets"
            dom_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE ?
                  AND ds.source IN ('national', 'fao', 'cpi')
                ORDER BY dp.date ASC
                """,
                (country, f"%{commodity}%domestic%price%"),
            )
            world_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.source IN ('wb_commodity', 'imf', 'fred')
                  AND ds.name LIKE ?
                ORDER BY dp.date ASC
                """,
                (f"%{commodity}%world%price%",),
            )

            if dom_rows and world_rows:
                markets["domestic"] = {
                    "dates": [r["date"] for r in dom_rows],
                    "values": [r["value"] for r in dom_rows],
                }
                markets["international"] = {
                    "dates": [r["date"] for r in world_rows],
                    "values": [r["value"] for r in world_rows],
                }

        if len(markets) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "need >= 2 market price series",
            }

        market_names = list(markets.keys())
        # Convert to numpy
        for m in market_names:
            markets[m]["values"] = np.array(markets[m]["values"], dtype=float)

        # Select reference market (first if not specified, or largest by obs)
        if ref_market and ref_market in markets:
            ref = ref_market
        else:
            ref = max(market_names, key=lambda m: len(markets[m]["values"]))

        # 1. Pairwise LOP tests
        lop_results = {}
        integration_indices = []

        for market in market_names:
            if market == ref:
                continue

            # Align series
            ref_dates = set(markets[ref]["dates"])
            mkt_dates = set(markets[market]["dates"])
            common = sorted(ref_dates & mkt_dates)

            if len(common) < 12:
                lop_results[market] = {"status": "insufficient_overlap"}
                continue

            ref_idx = {d: i for i, d in enumerate(markets[ref]["dates"])}
            mkt_idx = {d: i for i, d in enumerate(markets[market]["dates"])}

            p_ref = np.array([markets[ref]["values"][ref_idx[d]] for d in common])
            p_mkt = np.array([markets[market]["values"][mkt_idx[d]] for d in common])

            # Filter valid
            valid = (p_ref > 0) & (p_mkt > 0)
            p_ref = p_ref[valid]
            p_mkt = p_mkt[valid]

            if len(p_ref) < 12:
                lop_results[market] = {"status": "insufficient_valid_obs"}
                continue

            # LOP test: cointegration of log prices
            ln_ref = np.log(p_ref)
            ln_mkt = np.log(p_mkt)

            coint = self._cointegration_test(ln_ref, ln_mkt)

            # Ravallion integration index
            ravallion = self._ravallion_index(p_mkt, p_ref)

            # Price differential analysis
            diff = p_mkt - p_ref
            abs_diff = np.abs(diff)
            mean_abs_diff = float(np.mean(abs_diff))
            cv_diff = float(np.std(diff) / np.mean(p_ref)) if np.mean(p_ref) > 0 else 0

            # Half-life of price deviations
            half_life = self._half_life(diff)

            # Transaction cost band estimation
            tc_band = self._estimate_tc_band(diff)

            lop_results[market] = {
                "status": "ok",
                "n_obs": len(p_ref),
                "cointegration": coint,
                "ravallion_index": ravallion,
                "mean_abs_price_differential": round(mean_abs_diff, 4),
                "cv_price_differential": round(cv_diff, 4),
                "half_life_periods": half_life,
                "transaction_cost_band": tc_band,
            }

            if ravallion.get("long_run_index") is not None:
                integration_indices.append(ravallion["long_run_index"])

        # 2. Variance ratio test (spatial dispersion)
        variance_ratio = self._variance_ratio_test(markets, market_names)

        # Aggregate score
        if integration_indices:
            mean_index = float(np.mean(integration_indices))
            # Perfect integration = 1.0, no integration = 0.0
            # Score: (1 - integration) * 100
            integration_component = max(0, min(70, (1.0 - min(mean_index, 1.0)) * 70))
        else:
            integration_component = 50.0
            mean_index = None

        # Variance ratio component
        vr_component = 0.0
        if variance_ratio and variance_ratio.get("ratio") is not None:
            # High ratio = poor integration
            vr = variance_ratio["ratio"]
            vr_component = max(0, min(30, vr * 15))

        score = integration_component + vr_component

        return {
            "score": round(max(0.0, min(100.0, score)), 2),
            "country": country,
            "commodity": commodity,
            "reference_market": ref,
            "n_markets": len(market_names),
            "mean_integration_index": round(mean_index, 4) if mean_index is not None else None,
            "pairwise_results": lop_results,
            "variance_ratio": variance_ratio,
        }

    @staticmethod
    def _cointegration_test(y: np.ndarray, x: np.ndarray) -> dict:
        """Engle-Granger cointegration test between two price series."""
        n = len(y)
        X = np.column_stack([np.ones(n), x])
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - X @ beta

        # ADF on residuals
        dr = np.diff(resid)
        r_lag = resid[:-1]
        try:
            rho = np.linalg.lstsq(r_lag.reshape(-1, 1), dr, rcond=None)[0][0]
        except np.linalg.LinAlgError:
            return {"cointegrated": False, "error": "estimation_failed"}

        resid_adf = dr - r_lag * rho
        sigma2 = float(np.sum(resid_adf ** 2)) / max(len(dr) - 1, 1)
        se = float(np.sqrt(sigma2 / max(np.sum(r_lag ** 2), 1e-10)))
        adf_stat = rho / se if se > 0 else 0.0

        return {
            "beta": round(float(beta[1]), 4),
            "adf_statistic": round(float(adf_stat), 4),
            "cointegrated": bool(adf_stat < -3.37),
        }

    @staticmethod
    def _ravallion_index(p_local: np.ndarray, p_ref: np.ndarray) -> dict:
        """Ravallion (1986) market integration index.

        P_i,t = a + b1*P_i,{t-1} + b2*P_ref,t + b3*P_ref,{t-1} + e
        Short-run: b2
        Long-run: (b2 + b3) / (1 - b1)
        """
        T = len(p_local) - 1
        if T < 8:
            return {"status": "insufficient_obs"}

        dep = p_local[1:]
        X = np.column_stack([
            np.ones(T),
            p_local[:-1],       # lagged local price
            p_ref[1:],          # current reference price
            p_ref[:-1],         # lagged reference price
        ])

        try:
            beta = np.linalg.lstsq(X, dep, rcond=None)[0]
        except np.linalg.LinAlgError:
            return {"status": "estimation_failed"}

        b0, b1, b2, b3 = beta

        # Long-run integration index
        denom = 1.0 - b1
        if abs(denom) > 0.01:
            lr_index = (b2 + b3) / denom
        else:
            lr_index = None  # near unit root in local price

        resid = dep - X @ beta
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((dep - np.mean(dep)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        return {
            "short_run_index": round(float(b2), 4),
            "long_run_index": round(float(lr_index), 4) if lr_index is not None else None,
            "b1_persistence": round(float(b1), 4),
            "r_squared": round(r2, 4),
            "n_obs": T,
        }

    @staticmethod
    def _half_life(deviations: np.ndarray) -> float | None:
        """Estimate half-life of price deviations via AR(1).

        d_t = rho * d_{t-1} + e_t
        Half-life = ln(0.5) / ln(|rho|)
        """
        if len(deviations) < 5:
            return None

        d_lag = deviations[:-1]
        d_curr = deviations[1:]

        if np.std(d_lag) < 1e-10:
            return None

        try:
            rho = float(np.linalg.lstsq(d_lag.reshape(-1, 1), d_curr, rcond=None)[0][0])
        except np.linalg.LinAlgError:
            return None

        if abs(rho) >= 1.0 or abs(rho) < 1e-10:
            return None

        hl = np.log(0.5) / np.log(abs(rho))
        return round(float(hl), 2)

    @staticmethod
    def _estimate_tc_band(diff: np.ndarray) -> dict:
        """Estimate transaction cost bands from price differentials.

        Uses a simple regime classification:
        - Within band: |diff| < tau (integrated, no arbitrage profit)
        - Outside band: |diff| >= tau (potential arbitrage)

        Estimates tau as the median absolute deviation.
        """
        abs_diff = np.abs(diff)
        tau = float(np.median(abs_diff)) * 1.4826  # MAD estimator of sigma

        within = np.sum(abs_diff < tau)
        outside = np.sum(abs_diff >= tau)
        total = len(diff)

        return {
            "estimated_band_width": round(tau, 4),
            "pct_within_band": round(float(within / total * 100), 2) if total > 0 else 0,
            "pct_outside_band": round(float(outside / total * 100), 2) if total > 0 else 0,
            "n_obs": total,
        }

    @staticmethod
    def _variance_ratio_test(markets: dict, market_names: list) -> dict | None:
        """Spatial price dispersion: variance ratio test.

        Ratio = var(across markets) / mean(var(within market))
        Low ratio = well integrated; high ratio = segmented.
        """
        if len(market_names) < 2:
            return None

        # Find common dates across all markets
        all_dates = set(markets[market_names[0]]["dates"])
        for m in market_names[1:]:
            all_dates &= set(markets[m]["dates"])
        common = sorted(all_dates)

        if len(common) < 5:
            return None

        # Build price matrix: rows=dates, cols=markets
        n_markets = len(market_names)
        price_matrix = np.zeros((len(common), n_markets))
        for j, m in enumerate(market_names):
            idx_map = {d: i for i, d in enumerate(markets[m]["dates"])}
            for t, d in enumerate(common):
                if d in idx_map:
                    price_matrix[t, j] = markets[m]["values"][idx_map[d]]

        # Remove rows with zeros
        valid_rows = np.all(price_matrix > 0, axis=1)
        price_matrix = price_matrix[valid_rows]

        if len(price_matrix) < 5:
            return None

        # Variance across markets at each time point
        var_across = np.mean(np.var(price_matrix, axis=1))
        # Mean variance within each market over time
        var_within = np.mean(np.var(price_matrix, axis=0))

        ratio = var_across / var_within if var_within > 0 else float("inf")

        # Coefficient of variation across markets (mean over time)
        mean_prices = np.mean(price_matrix, axis=1)
        cv_across = float(np.mean(np.std(price_matrix, axis=1) / mean_prices)) if np.all(mean_prices > 0) else None

        return {
            "ratio": round(float(ratio), 4),
            "var_across_markets": round(float(var_across), 4),
            "var_within_markets": round(float(var_within), 4),
            "cv_across_markets": round(float(cv_across), 4) if cv_across is not None else None,
            "n_markets": n_markets,
            "n_common_periods": len(price_matrix),
        }
