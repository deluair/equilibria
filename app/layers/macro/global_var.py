"""Global VAR (GVAR) - Pesaran et al. (2004) model.

Methodology
-----------
The GVAR model links country-specific VARs through trade-weighted foreign variables.

For each country i, the VARX* model:
    x_{i,t} = a_i + B_{i,1} * x_{i,t-1} + ... + B_{i,p} * x_{i,t-p}
             + L_{i,0} * x*_{i,t} + L_{i,1} * x*_{i,t-1} + ... + e_{i,t}

where:
    x_{i,t}  = domestic variables for country i (GDP growth, inflation, interest rate)
    x*_{i,t} = trade-weighted foreign variables: x*_{i,t} = sum_j w_{ij} * x_{j,t}
    w_{ij}   = trade weight of country j for country i (bilateral trade / total trade)

The system is solved simultaneously:
    z_t = [x_{1,t}', ..., x_{N,t}']'

**Generalized Impulse Response Functions** (GIRF, Pesaran & Shin 1998):
    GIRF does not depend on ordering of variables (unlike Cholesky).
    Response of z to a unit shock in variable j of country i,
    scaled by the standard deviation of that shock.

**Global shock transmission**: trace how a shock in one country propagates
to others through trade linkages.

References:
- Pesaran, Schuermann & Weiner (2004), "Modeling Regional Interdependencies
  Using a Global Error-Correcting Macroeconometric Model," JBES
- Dees, di Mauro, Pesaran & Smith (2007), "Exploring the International Linkages
  of the Euro Area: A Global VAR Analysis," J. Applied Econometrics
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GlobalVAR(LayerBase):
    layer_id = "l2"
    name = "Global VAR"
    weight = 0.05

    # Default country set and domestic variables
    DEFAULT_COUNTRIES = ["USA", "CHN", "DEU", "JPN", "GBR", "IND", "BRA", "KOR"]
    DOMESTIC_VARS = ["gdp_growth", "inflation", "interest_rate"]

    async def compute(self, db, **kwargs) -> dict:
        countries = kwargs.get("countries", self.DEFAULT_COUNTRIES)
        lags_domestic = kwargs.get("lags_domestic", 2)
        lags_foreign = kwargs.get("lags_foreign", 1)
        irf_horizon = kwargs.get("irf_horizon", 20)
        shock_country = kwargs.get("shock_country", "USA")
        shock_variable = kwargs.get("shock_variable", "gdp_growth")

        results = {"countries": countries, "n_countries": len(countries)}

        # Fetch trade weights
        weights = await self._fetch_trade_weights(db, countries)
        results["trade_weights"] = {
            i: {j: round(w, 4) for j, w in ws.items()}
            for i, ws in weights.items()
        }

        # Fetch country data
        country_data = await self._fetch_country_data(db, countries)
        available = [c for c in countries if c in country_data]

        if len(available) < 2:
            return {"score": 50.0, "results": results, "note": "insufficient country data"}

        results["available_countries"] = available

        # Align data to common time period
        aligned = self._align_data(country_data, available)
        if aligned is None or aligned["T"] < lags_domestic + 10:
            return {"score": 50.0, "results": results, "note": "insufficient aligned data"}

        T = aligned["T"]
        results["n_obs"] = T
        results["period"] = f"{aligned['dates'][0]} to {aligned['dates'][-1]}"

        # Construct foreign variables (trade-weighted)
        foreign_vars = self._construct_foreign_vars(aligned, weights, available)

        # Estimate country-specific VARX* models
        country_models = {}
        for c in available:
            model = self._estimate_varx(
                aligned["data"][c], foreign_vars[c],
                lags_domestic, lags_foreign
            )
            if model is not None:
                country_models[c] = model

        if len(country_models) < 2:
            return {"score": 50.0, "results": results, "note": "too few models estimated"}

        results["models_estimated"] = len(country_models)

        # Individual model diagnostics
        model_diag = {}
        for c, model in country_models.items():
            model_diag[c] = {
                "r_squared": {v: round(r2, 4) for v, r2 in model["r_squared"].items()},
                "n_obs": model["n_obs"],
            }
        results["model_diagnostics"] = model_diag

        # Stack into global system
        global_system = self._stack_global_system(
            country_models, aligned, weights, available, lags_domestic
        )

        if global_system is None:
            return {"score": 50.0, "results": results, "note": "global system construction failed"}

        # Generalized IRFs
        girfs = self._compute_girf(
            global_system, available, shock_country, shock_variable, irf_horizon
        )

        if girfs is not None:
            results["girf"] = {
                "shock_country": shock_country,
                "shock_variable": shock_variable,
                "responses": girfs,
            }

            # Transmission analysis
            transmission = self._analyze_transmission(girfs, available, shock_country)
            results["transmission"] = transmission

        # Spillover table: peak response of each country's GDP to shock
        spillovers = self._compute_spillover_table(
            global_system, available, irf_horizon
        )
        if spillovers:
            results["spillover_matrix"] = spillovers

        # Score: based on shock propagation intensity and current volatility
        score = self._compute_score(results, global_system, available)

        return {"score": round(score, 1), "results": results}

    async def _fetch_trade_weights(self, db, countries: list[str]) -> dict:
        """Fetch bilateral trade weights from data."""
        weights = {}
        for c in countries:
            rows = await db.execute_fetchall(
                """
                SELECT partner_code, value FROM data_points
                WHERE series_id = (SELECT id FROM data_series WHERE code = ?)
                ORDER BY date DESC LIMIT ?
                """,
                (f"TRADE_WEIGHT_{c}", len(countries)),
            )

            if rows:
                w = {}
                for r in rows:
                    partner = r[0] if r[0] else ""
                    if partner in countries and partner != c:
                        w[partner] = float(r[1])
                # Normalize
                total = sum(w.values())
                if total > 0:
                    weights[c] = {k: v / total for k, v in w.items()}
                else:
                    weights[c] = self._default_weights(c, countries)
            else:
                weights[c] = self._default_weights(c, countries)

        return weights

    @staticmethod
    def _default_weights(country: str, countries: list[str]) -> dict:
        """Equal weights as fallback."""
        others = [c for c in countries if c != country]
        if not others:
            return {}
        w = 1.0 / len(others)
        return {c: w for c in others}

    async def _fetch_country_data(self, db, countries: list[str]) -> dict:
        """Fetch domestic variables for each country."""
        series_map = {
            "gdp_growth": "REAL_GROWTH_{}",
            "inflation": "INFLATION_{}",
            "interest_rate": "POLICY_RATE_{}",
        }

        country_data = {}
        for c in countries:
            data = {}
            for var_name, code_tmpl in series_map.items():
                rows = await db.execute_fetchall(
                    "SELECT date, value FROM data_points WHERE series_id = "
                    "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                    (code_tmpl.format(c),),
                )
                if rows:
                    data[var_name] = {
                        "dates": [r[0] for r in rows],
                        "values": np.array([float(r[1]) for r in rows]),
                    }

            if len(data) >= 2:  # at least 2 of 3 variables
                country_data[c] = data

        return country_data

    @staticmethod
    def _align_data(country_data: dict, countries: list[str]) -> dict | None:
        """Align all country data to common dates."""
        # Find common dates across all countries and variables
        all_date_sets = []
        for c in countries:
            for var_name, var_data in country_data[c].items():
                all_date_sets.append(set(var_data["dates"]))

        if not all_date_sets:
            return None

        common_dates = sorted(set.intersection(*all_date_sets))
        if len(common_dates) < 10:
            return None

        # Build aligned data matrices
        aligned = {"dates": common_dates, "T": len(common_dates), "data": {}}
        n_vars = 3  # gdp_growth, inflation, interest_rate

        for c in countries:
            mat = np.zeros((len(common_dates), n_vars))
            var_names = ["gdp_growth", "inflation", "interest_rate"]
            for j, var_name in enumerate(var_names):
                if var_name in country_data[c]:
                    date_map = dict(zip(
                        country_data[c][var_name]["dates"],
                        country_data[c][var_name]["values"],
                    ))
                    for i, d in enumerate(common_dates):
                        mat[i, j] = date_map.get(d, 0.0)

            aligned["data"][c] = mat

        return aligned

    @staticmethod
    def _construct_foreign_vars(aligned: dict, weights: dict,
                                countries: list[str]) -> dict:
        """Construct trade-weighted foreign variables for each country."""
        foreign = {}
        n_vars = 3

        for c in countries:
            x_star = np.zeros((aligned["T"], n_vars))
            w = weights.get(c, {})

            for partner in countries:
                if partner == c or partner not in w:
                    continue
                x_star += w[partner] * aligned["data"].get(partner, np.zeros((aligned["T"], n_vars)))

            foreign[c] = x_star

        return foreign

    @staticmethod
    def _estimate_varx(domestic: np.ndarray, foreign: np.ndarray,
                       p: int, q: int) -> dict | None:
        """Estimate VARX*(p, q) for a single country via OLS."""
        T, k = domestic.shape
        k_star = foreign.shape[1]

        if T <= p + q + k + k_star:
            return None

        start = max(p, q)
        n_obs = T - start
        Y = domestic[start:]

        # Build regressors: constant + lagged domestic + current & lagged foreign
        X = np.ones((n_obs, 1))

        for lag in range(1, p + 1):
            X = np.hstack([X, domestic[start - lag:T - lag]])

        for lag in range(0, q + 1):
            if lag == 0:
                X = np.hstack([X, foreign[start:]])
            else:
                X = np.hstack([X, foreign[start - lag:T - lag]])

        # OLS
        try:
            B = np.linalg.lstsq(X, Y, rcond=None)[0]
        except np.linalg.LinAlgError:
            return None

        residuals = Y - X @ B
        Sigma = (residuals.T @ residuals) / n_obs

        # R-squared per equation
        r_squared = {}
        var_names = ["gdp_growth", "inflation", "interest_rate"]
        for j in range(k):
            sst = float(np.sum((Y[:, j] - np.mean(Y[:, j])) ** 2))
            sse = float(np.sum(residuals[:, j] ** 2))
            r_squared[var_names[j]] = max(1 - sse / sst, 0.0) if sst > 0 else 0.0

        return {
            "B": B,
            "Sigma": Sigma,
            "residuals": residuals,
            "n_obs": n_obs,
            "k": k,
            "k_star": k_star,
            "p": p,
            "q": q,
            "r_squared": r_squared,
        }

    def _stack_global_system(self, models: dict, aligned: dict,
                             weights: dict, countries: list[str],
                             p: int) -> dict | None:
        """Stack country models into global system for IRF computation."""
        N = len(countries)
        k = 3  # vars per country
        K = N * k  # total system dimension

        # Build global weight matrix W (K x K)
        W = np.zeros((K, K))
        for i, c_i in enumerate(countries):
            if c_i not in models:
                continue
            w = weights.get(c_i, {})
            for j, c_j in enumerate(countries):
                if c_j == c_i:
                    continue
                wt = w.get(c_j, 0.0)
                W[i * k:(i + 1) * k, j * k:(j + 1) * k] = wt * np.eye(k)

        # Build global companion form
        # Simplified: extract lag-1 coefficients and construct global VAR(1)
        G = np.zeros((K, K))
        Sigma_global = np.zeros((K, K))

        for i, c in enumerate(countries):
            if c not in models:
                continue
            model = models[c]
            B = model["B"]

            # Domestic lag-1 coefficients (rows 1:k+1 of B, after constant)
            if B.shape[0] > k:
                G[i * k:(i + 1) * k, i * k:(i + 1) * k] = B[1:k + 1, :].T

            # Foreign contemporaneous effect via W
            foreign_start = 1 + k * model["p"]
            if B.shape[0] > foreign_start + k:
                Lambda_0 = B[foreign_start:foreign_start + k, :].T
                for j, c_j in enumerate(countries):
                    if c_j == c:
                        continue
                    wt = weights.get(c, {}).get(c_j, 0.0)
                    G[i * k:(i + 1) * k, j * k:(j + 1) * k] += Lambda_0 * wt

            # Block of Sigma
            Sigma_global[i * k:(i + 1) * k, i * k:(i + 1) * k] = model["Sigma"]

        return {
            "G": G,
            "Sigma": Sigma_global,
            "K": K,
            "k": k,
            "N": N,
            "countries": countries,
        }

    def _compute_girf(self, system: dict, countries: list[str],
                      shock_country: str, shock_variable: str,
                      horizon: int) -> dict | None:
        """Compute Generalized IRFs (Pesaran & Shin 1998)."""
        G = system["G"]
        Sigma = system["Sigma"]
        K = system["K"]
        k = system["k"]

        if shock_country not in countries:
            return None

        var_idx_map = {"gdp_growth": 0, "inflation": 1, "interest_rate": 2}
        var_idx = var_idx_map.get(shock_variable, 0)

        c_idx = countries.index(shock_country)
        j = c_idx * k + var_idx  # position in global vector

        # GIRF: response to a one-standard-deviation shock
        sigma_jj = max(Sigma[j, j], 1e-12)
        delta = Sigma[:, j] / np.sqrt(sigma_jj)  # scaled shock impact

        # Propagate
        responses = {}
        G_power = np.eye(K)
        for c in countries:
            responses[c] = {v: [] for v in var_idx_map}

        for h in range(horizon):
            response_h = G_power @ delta
            for c_i, c in enumerate(countries):
                for v_name, v_j in var_idx_map.items():
                    idx = c_i * k + v_j
                    responses[c][v_name].append(round(float(response_h[idx]), 6))
            G_power = G_power @ G

        return responses

    @staticmethod
    def _analyze_transmission(girfs: dict, countries: list[str],
                              shock_country: str) -> dict:
        """Analyze how shocks transmit across countries."""
        transmission = {}

        for c in countries:
            if c == shock_country:
                continue
            gdp_resp = girfs.get(c, {}).get("gdp_growth", [])
            if gdp_resp:
                peak_response = max(abs(v) for v in gdp_resp)
                peak_period = int(np.argmax([abs(v) for v in gdp_resp]))
                cumulative = sum(gdp_resp)
                transmission[c] = {
                    "peak_gdp_response": round(peak_response, 6),
                    "peak_period": peak_period,
                    "cumulative_gdp_response": round(cumulative, 6),
                    "sign": "positive" if cumulative > 0 else "negative",
                }

        # Rank by impact
        sorted_countries = sorted(
            transmission.items(),
            key=lambda x: abs(x[1]["cumulative_gdp_response"]),
            reverse=True,
        )
        ranking = {c: i + 1 for i, (c, _) in enumerate(sorted_countries)}
        for c in transmission:
            transmission[c]["impact_rank"] = ranking.get(c)

        return transmission

    def _compute_spillover_table(self, system: dict, countries: list[str],
                                 horizon: int) -> dict:
        """Compute GDP spillover matrix: peak GDP response of each country
        to a GDP shock in each other country."""
        table = {}

        for shock_c in countries:
            girfs = self._compute_girf(system, countries, shock_c, "gdp_growth", horizon)
            if girfs is None:
                continue

            row = {}
            for resp_c in countries:
                gdp_resp = girfs.get(resp_c, {}).get("gdp_growth", [])
                if gdp_resp:
                    row[resp_c] = round(max(abs(v) for v in gdp_resp), 6)
                else:
                    row[resp_c] = 0.0

            table[shock_c] = row

        return table

    @staticmethod
    def _compute_score(results: dict, system: dict | None, countries: list[str]) -> float:
        """Score based on global interconnectedness and shock vulnerability."""
        score = 20.0  # baseline

        # High spillover intensity
        spillovers = results.get("spillover_matrix", {})
        if spillovers:
            # Average off-diagonal spillover
            total_spill = 0.0
            count = 0
            for shock_c, row in spillovers.items():
                for resp_c, val in row.items():
                    if shock_c != resp_c:
                        total_spill += val
                        count += 1
            avg_spill = total_spill / max(count, 1)
            if avg_spill > 0.5:
                score += 25
            elif avg_spill > 0.2:
                score += 15
            elif avg_spill > 0.1:
                score += 5

        # Eigenvalue stability of global system
        if system is not None:
            try:
                eigs = np.linalg.eigvals(system["G"])
                max_eig = float(max(abs(e) for e in eigs))
                if max_eig > 0.99:
                    score += 30  # near unit root = persistent shocks
                elif max_eig > 0.9:
                    score += 15
            except Exception:
                pass

        # Few countries available = limited coverage
        if len(countries) < 4:
            score += 10

        return min(score, 100)
