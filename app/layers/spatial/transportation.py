"""Transportation economics analysis.

Gravity model for commuting flows, congestion externality estimation, value of
travel time savings (VTTS), and modal choice modeling via conditional logit.

Commuting gravity model:
    C_ij = exp(b0 + b1*ln(emp_j) + b2*ln(pop_i) + b3*ln(time_ij)
               + b4*cost_ij) * eta_ij

where C_ij is the commuting flow from residential zone i to employment zone j.

Congestion externalities (Small & Verhoef 2007):
    Marginal external cost = t'(V) * V * VTTS
    where t(V) is the travel time function (BPR: t = t0 * (1 + a*(V/C)^b)),
    V is volume, C is capacity.

Value of travel time savings (VTTS):
    VTTS = -beta_time / beta_cost from a discrete choice model,
    typically 40-80% of the wage rate (Small 2012).

Modal choice (McFadden 1974):
    P(mode m) = exp(V_m) / sum_j exp(V_j)
    V_m = b_time * time_m + b_cost * cost_m + ASC_m

References:
    McFadden, D. (1974). Conditional Logit Analysis of Qualitative Choice
        Behavior. In Frontiers in Econometrics, Academic Press.
    Small, K. & Verhoef, E. (2007). The Economics of Urban Transportation.
    Small, K. (2012). Valuation of Travel Time. Economics of Transportation 1(1).

Score: high congestion cost + low VTTS recovery + poor modal balance -> STRESS.
"""

import json

import numpy as np
from scipy.optimize import minimize

from app.layers.base import LayerBase


class TransportEconomics(LayerBase):
    layer_id = "l11"
    name = "Transport Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # --- Commuting gravity model ---
        commute_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'commuting'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        gravity_result = None
        if commute_rows and len(commute_rows) >= 10:
            flows = []
            gravity_X = []
            for row in commute_rows:
                flow = row["value"]
                if flow is None or flow <= 0:
                    continue
                meta = json.loads(row["metadata"]) if row.get("metadata") else {}
                emp = meta.get("employment_dest")
                pop = meta.get("population_origin")
                time = meta.get("travel_time")
                cost = meta.get("travel_cost")
                if not all(v is not None and v > 0 for v in [emp, pop, time]):
                    continue
                flows.append(flow)
                gravity_X.append([
                    1.0,
                    np.log(emp),
                    np.log(pop),
                    np.log(time),
                    float(cost) if cost else 0.0,
                ])

            if len(flows) >= 10:
                y_g = np.array(flows)
                X_g = np.array(gravity_X)
                # PPML via IRLS
                beta_g, pr2_g, iters_g = self._ppml(X_g, y_g)
                coef_names = ["constant", "ln_employment", "ln_population",
                              "ln_travel_time", "travel_cost"]
                gravity_result = {
                    "coefficients": dict(zip(coef_names, beta_g.tolist())),
                    "pseudo_r2": round(pr2_g, 4),
                    "iterations": iters_g,
                    "n_obs": len(flows),
                    "time_elasticity": round(float(beta_g[3]), 4),
                }

        # --- Congestion externalities ---
        congestion_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'congestion'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        congestion_result = None
        if congestion_rows and len(congestion_rows) >= 5:
            segments = []
            for row in congestion_rows:
                meta = json.loads(row["metadata"]) if row.get("metadata") else {}
                volume = meta.get("volume")
                capacity = meta.get("capacity")
                free_flow_time = meta.get("free_flow_time")
                vtts = meta.get("vtts")
                if not all(v is not None and v > 0 for v in [volume, capacity, free_flow_time]):
                    continue
                segments.append({
                    "volume": volume,
                    "capacity": capacity,
                    "t0": free_flow_time,
                    "vtts": vtts if vtts else 20.0,  # Default $20/hr
                })

            if len(segments) >= 5:
                # BPR function: t = t0 * (1 + 0.15 * (V/C)^4)
                total_mec = 0.0
                total_delay = 0.0
                for s in segments:
                    vc_ratio = s["volume"] / s["capacity"]
                    t_actual = s["t0"] * (1.0 + 0.15 * vc_ratio ** 4)
                    # Marginal: dt/dV * V * VTTS
                    dt_dv = s["t0"] * 0.15 * 4 * vc_ratio ** 3 / s["capacity"]
                    mec = dt_dv * s["volume"] * s["vtts"]
                    total_mec += mec
                    total_delay += (t_actual - s["t0"]) * s["volume"]

                congestion_result = {
                    "n_segments": len(segments),
                    "total_marginal_external_cost": round(total_mec, 2),
                    "avg_marginal_external_cost": round(total_mec / len(segments), 2),
                    "total_delay_hours": round(total_delay, 1),
                    "avg_vc_ratio": round(
                        float(np.mean([s["volume"] / s["capacity"] for s in segments])), 3
                    ),
                }

        # --- Modal choice (conditional logit) ---
        modal_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'modal_choice'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        modal_result = None
        vtts_estimate = None
        if modal_rows and len(modal_rows) >= 20:
            choices = []
            for row in modal_rows:
                meta = json.loads(row["metadata"]) if row.get("metadata") else {}
                chosen = int(row["value"]) if row["value"] is not None else None
                modes = meta.get("modes")  # list of {time, cost} per mode
                if chosen is None or modes is None or len(modes) < 2:
                    continue
                choices.append({"chosen": chosen, "modes": modes})

            if len(choices) >= 20:
                modal_result, vtts_estimate = self._conditional_logit(choices)

        # --- Score ---
        scores = []

        if congestion_result:
            vc = congestion_result["avg_vc_ratio"]
            if vc > 1.0:
                scores.append(85.0)
            elif vc > 0.85:
                scores.append(60.0)
            elif vc > 0.7:
                scores.append(35.0)
            else:
                scores.append(15.0)

        if gravity_result:
            # Time elasticity: more negative = higher sensitivity to congestion
            te = abs(gravity_result["time_elasticity"])
            scores.append(min(100.0, te * 60.0))

        if modal_result:
            # Low modal diversity is stress
            hhi_modal = sum(s ** 2 for s in modal_result.get("mode_shares", {}).values())
            scores.append(min(100.0, hhi_modal * 150.0))

        score = float(np.mean(scores)) if scores else 50.0
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "commuting_gravity": gravity_result,
            "congestion": congestion_result,
            "modal_choice": modal_result,
            "vtts": round(vtts_estimate, 2) if vtts_estimate else None,
        }

    @staticmethod
    def _ppml(X: np.ndarray, y: np.ndarray, max_iter: int = 50, tol: float = 1e-8):
        """PPML via IRLS."""
        n, k = X.shape
        beta = np.zeros(k)
        beta[0] = np.log(np.mean(y)) if np.mean(y) > 0 else 0.0

        for i in range(max_iter):
            mu = np.exp(X @ beta)
            mu = np.clip(mu, 1e-10, 1e20)
            z = X @ beta + (y - mu) / mu
            W = mu
            XtWX = X.T @ (X * W[:, None])
            XtWz = X.T @ (W * z)
            try:
                beta_new = np.linalg.solve(XtWX, XtWz)
            except np.linalg.LinAlgError:
                return beta, 0.0, i + 1
            if np.max(np.abs(beta_new - beta)) < tol:
                beta = beta_new
                mu = np.exp(X @ beta)
                mu = np.clip(mu, 1e-10, 1e20)
                mask = y > 0
                dev_full = 2.0 * np.sum(y[mask] * np.log(y[mask] / mu[mask]) - (y[mask] - mu[mask]))
                mu_null = np.mean(y)
                dev_null = 2.0 * np.sum(
                    y[mask] * np.log(y[mask] / mu_null) - (y[mask] - mu_null)
                )
                pr2 = 1.0 - dev_full / dev_null if dev_null > 0 else 0.0
                return beta, max(0.0, min(1.0, pr2)), i + 1
            beta = beta_new

        return beta, 0.0, max_iter

    @staticmethod
    def _conditional_logit(choices: list[dict]) -> tuple[dict | None, float | None]:
        """Estimate conditional logit for modal choice.

        Returns modal result dict and VTTS estimate.
        """
        # Count modes from first observation
        n_modes = len(choices[0]["modes"])
        n_obs = len(choices)

        # Build data: each observation has n_modes alternatives
        times = np.zeros((n_obs, n_modes))
        costs = np.zeros((n_obs, n_modes))
        chosen_idx = np.zeros(n_obs, dtype=int)

        for i, ch in enumerate(choices):
            chosen_idx[i] = ch["chosen"]
            for m in range(min(n_modes, len(ch["modes"]))):
                times[i, m] = ch["modes"][m].get("time", 0)
                costs[i, m] = ch["modes"][m].get("cost", 0)

        # Negative log-likelihood
        def neg_ll(params):
            b_time, b_cost = params
            V = b_time * times + b_cost * costs
            V = V - V.max(axis=1, keepdims=True)  # numerical stability
            exp_V = np.exp(V)
            prob = exp_V / exp_V.sum(axis=1, keepdims=True)
            ll = 0.0
            for i in range(n_obs):
                p = prob[i, chosen_idx[i]]
                ll += np.log(max(p, 1e-15))
            return -ll

        result = minimize(neg_ll, x0=[-0.05, -0.03], method="Nelder-Mead")
        b_time, b_cost = result.x

        # VTTS = -b_time / b_cost (in $/hr if cost is $ and time is hours)
        vtts = -b_time / b_cost if abs(b_cost) > 1e-10 else None

        # Predicted mode shares
        V = b_time * times + b_cost * costs
        V = V - V.max(axis=1, keepdims=True)
        exp_V = np.exp(V)
        probs = exp_V / exp_V.sum(axis=1, keepdims=True)
        mode_shares = {f"mode_{m}": round(float(probs[:, m].mean()), 4) for m in range(n_modes)}

        modal_result = {
            "beta_time": round(float(b_time), 6),
            "beta_cost": round(float(b_cost), 6),
            "n_obs": n_obs,
            "n_modes": n_modes,
            "log_likelihood": round(-result.fun, 2),
            "mode_shares": mode_shares,
        }

        return modal_result, vtts
