"""Inflation Targeting analysis module.

Methodology
-----------
1. **IT Adoption Effects**:
   - Compare inflation level, volatility, and persistence before/after IT adoption
   - Difference-in-differences: IT adopters vs non-adopters
   - Ball and Sheridan (2005) critique: regression to mean may explain
     apparent benefits of IT

2. **Credibility Index**:
   - Spread between long-run inflation expectations and target
   - Anchoring coefficient: how much do expectations respond to shocks?
   - Credibility = 1 - |E[pi] - pi*| / pi* (bounded 0-1)
   - Demertzis, Marcellino, Viegi (2012) approach

3. **Sacrifice Ratio**:
   - Cumulative output loss per 1pp permanent reduction in inflation
   - Ball (1994) method: identify disinflation episodes, compute
     cumulative gap, divide by change in trend inflation
   - Typical range: 1-3 for developed countries, higher for EM

4. **Ball-Sheridan Critique**:
   - Control for initial inflation level (regression to mean)
   - IT dummy insignificant after controlling for pre-adoption level
   - Test: delta_pi = a + b*pi_0 + c*IT_dummy + e

Score reflects credibility, sacrifice ratio, and expectation anchoring.

Sources: FRED (EXPINF1YR, EXPINF10YR, CPI, GDP, UNRATE)
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class InflationTargeting(LayerBase):
    layer_id = "l15"
    name = "Inflation Targeting"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        pi_target = kwargs.get("pi_target", 2.0)
        lookback = kwargs.get("lookback_years", 25)

        series_codes = {
            "inflation": f"INFLATION_{country}",
            "exp_1y": f"EXPINF_1Y_{country}",
            "exp_10y": f"EXPINF_10Y_{country}",
            "output_gap": f"OUTPUT_GAP_{country}",
            "gdp_growth": f"GDP_GROWTH_{country}",
        }

        data: dict[str, dict[str, float]] = {}
        for label, code in series_codes.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE series_id = ?) "
                "AND date >= date('now', ?) ORDER BY date",
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        if not data.get("inflation"):
            return {"score": 50.0, "results": {"error": "insufficient data"}}

        inf_dates = sorted(data["inflation"])
        pi = np.array([data["inflation"][d] for d in inf_dates])

        if len(pi) < 12:
            return {"score": 50.0, "results": {"error": "too few observations"}}

        results: dict = {
            "country": country,
            "pi_target": pi_target,
            "n_obs": len(pi),
            "period": f"{inf_dates[0]} to {inf_dates[-1]}",
        }

        # --- 1. IT Adoption Effects ---
        # Split sample at midpoint (proxy for IT adoption)
        mid = len(pi) // 2
        pi_pre = pi[:mid]
        pi_post = pi[mid:]

        results["adoption_effects"] = {
            "pre_mean": round(float(np.mean(pi_pre)), 3),
            "pre_std": round(float(np.std(pi_pre, ddof=1)), 3),
            "post_mean": round(float(np.mean(pi_post)), 3),
            "post_std": round(float(np.std(pi_post, ddof=1)), 3),
            "mean_reduction": round(float(np.mean(pi_pre) - np.mean(pi_post)), 3),
            "volatility_reduction": round(
                float(np.std(pi_pre, ddof=1) - np.std(pi_post, ddof=1)), 3
            ),
        }

        # Inflation persistence (AR(1) coefficient)
        if len(pi) > 4:
            ar1_full = float(np.corrcoef(pi[:-1], pi[1:])[0, 1])
            ar1_pre = (
                float(np.corrcoef(pi_pre[:-1], pi_pre[1:])[0, 1])
                if len(pi_pre) > 3 else None
            )
            ar1_post = (
                float(np.corrcoef(pi_post[:-1], pi_post[1:])[0, 1])
                if len(pi_post) > 3 else None
            )
        else:
            ar1_full = ar1_pre = ar1_post = None

        results["persistence"] = {
            "ar1_full": round(ar1_full, 3) if ar1_full is not None else None,
            "ar1_pre": round(ar1_pre, 3) if ar1_pre is not None else None,
            "ar1_post": round(ar1_post, 3) if ar1_post is not None else None,
            "persistence_declined": (
                ar1_post < ar1_pre if ar1_pre is not None and ar1_post is not None else None
            ),
        }

        # --- 2. Credibility Index ---
        if data.get("exp_10y"):
            exp_dates = sorted(set(data["exp_10y"]) & set(inf_dates))
            if len(exp_dates) >= 5:
                exp_10y = np.array([data["exp_10y"][d] for d in exp_dates])
                pi_matched = np.array([data["inflation"][d] for d in exp_dates])

                # Credibility = 1 - |E[pi] - pi*| / max(pi*, 0.5)
                credibility = np.clip(
                    1.0 - np.abs(exp_10y - pi_target) / max(pi_target, 0.5), 0, 1
                )

                # Anchoring: regress delta_E[pi] on delta_pi
                # Low coefficient = well anchored
                if len(exp_10y) > 5:
                    d_exp = np.diff(exp_10y)
                    d_pi = np.diff(pi_matched)
                    if np.std(d_pi) > 1e-10:
                        anchoring_coef = float(
                            np.polyfit(d_pi, d_exp, 1)[0]
                        )
                    else:
                        anchoring_coef = 0.0
                else:
                    anchoring_coef = None

                results["credibility"] = {
                    "index_latest": round(float(credibility[-1]), 3),
                    "index_mean": round(float(np.mean(credibility)), 3),
                    "expectation_latest": round(float(exp_10y[-1]), 3),
                    "deviation_from_target": round(float(exp_10y[-1] - pi_target), 3),
                    "anchoring_coefficient": (
                        round(anchoring_coef, 4) if anchoring_coef is not None else None
                    ),
                    "well_anchored": (
                        abs(anchoring_coef) < 0.3 if anchoring_coef is not None else None
                    ),
                }
            else:
                results["credibility"] = {"note": "insufficient expectations data"}
        else:
            # Fallback: use actual inflation deviation from target
            deviation = np.abs(pi - pi_target)
            cred_proxy = np.clip(1.0 - deviation / max(pi_target, 0.5), 0, 1)
            results["credibility"] = {
                "index_latest": round(float(cred_proxy[-1]), 3),
                "index_mean": round(float(np.mean(cred_proxy)), 3),
                "proxy_based": True,
            }

        # --- 3. Sacrifice Ratio ---
        # Identify disinflation episodes: 4+ quarter decline in trailing average
        window = 4
        if len(pi) >= window + 4:
            ma = np.convolve(pi, np.ones(window) / window, mode="valid")
            episodes = []
            in_episode = False
            start_idx = 0

            for j in range(1, len(ma)):
                if ma[j] < ma[j - 1] - 0.1 and not in_episode:
                    in_episode = True
                    start_idx = j - 1
                elif (ma[j] >= ma[j - 1] or j == len(ma) - 1) and in_episode:
                    in_episode = False
                    delta_pi = float(ma[start_idx] - ma[j])
                    if delta_pi > 0.5:
                        # Cumulative output loss during episode
                        if data.get("output_gap"):
                            gap_dates = sorted(data["output_gap"])
                            gap_arr = np.array([data["output_gap"][d] for d in gap_dates])
                            ep_start = min(start_idx + window - 1, len(inf_dates) - 1)
                            ep_end = min(j + window - 1, len(inf_dates) - 1)
                            ep_start_date = inf_dates[ep_start]
                            ep_end_date = inf_dates[ep_end]

                            cum_gap = 0.0
                            count = 0
                            for gi, gd in enumerate(gap_dates):
                                if ep_start_date <= gd <= ep_end_date:
                                    cum_gap += gap_arr[gi]
                                    count += 1
                            sacrifice = abs(cum_gap) / delta_pi if delta_pi > 0 else None
                        else:
                            cum_gap = 0.0
                            sacrifice = None

                        episodes.append({
                            "start_date": inf_dates[min(start_idx + window - 1, len(inf_dates) - 1)],
                            "end_date": inf_dates[min(j + window - 1, len(inf_dates) - 1)],
                            "inflation_decline_pp": round(delta_pi, 2),
                            "sacrifice_ratio": round(sacrifice, 2) if sacrifice is not None else None,
                        })

            if episodes:
                avg_sacrifice = np.mean(
                    [e["sacrifice_ratio"] for e in episodes if e["sacrifice_ratio"] is not None]
                )
                results["sacrifice_ratio"] = {
                    "episodes": episodes,
                    "average": round(float(avg_sacrifice), 2) if not np.isnan(avg_sacrifice) else None,
                    "n_episodes": len(episodes),
                }
            else:
                results["sacrifice_ratio"] = {"episodes": [], "n_episodes": 0}
        else:
            results["sacrifice_ratio"] = {"note": "insufficient data for episode detection"}

        # --- 4. Ball-Sheridan Critique ---
        # Test if IT adoption effect survives controlling for initial level
        # delta_pi = a + b*pi_0 + c*IT_dummy + e
        if len(pi) > 8:
            t_arr = np.arange(len(pi), dtype=float)
            it_dummy = np.where(t_arr >= mid, 1.0, 0.0)
            pi_initial = np.full(len(pi), pi[0])
            X_bs = np.column_stack([np.ones(len(pi)), pi_initial, it_dummy])
            beta_bs = np.linalg.lstsq(X_bs, pi, rcond=None)[0]
            resid_bs = pi - X_bs @ beta_bs
            se_bs = np.sqrt(np.diag(
                float(np.sum(resid_bs ** 2)) / max(len(pi) - 3, 1)
                * np.linalg.inv(X_bs.T @ X_bs)
            ))
            t_stat_it = float(beta_bs[2]) / float(se_bs[2]) if se_bs[2] > 1e-10 else 0.0
            p_val_it = 2.0 * (1.0 - sp_stats.t.cdf(abs(t_stat_it), len(pi) - 3))

            results["ball_sheridan"] = {
                "it_coefficient": round(float(beta_bs[2]), 3),
                "it_t_statistic": round(t_stat_it, 3),
                "it_p_value": round(p_val_it, 4),
                "it_significant_5pct": p_val_it < 0.05,
                "regression_to_mean_coef": round(float(beta_bs[1]), 3),
                "critique_supported": p_val_it >= 0.10,
            }
        else:
            results["ball_sheridan"] = {"note": "insufficient data"}

        # --- Score ---
        cred_score = results.get("credibility", {}).get("index_latest", 0.5)
        if isinstance(cred_score, (int, float)):
            cred_penalty = (1.0 - cred_score) * 30
        else:
            cred_penalty = 15.0

        vol_penalty = min(float(np.std(pi_post, ddof=1)) * 5, 20)

        persistence_penalty = 0.0
        if ar1_post is not None and ar1_post > 0.7:
            persistence_penalty = (ar1_post - 0.7) * 50

        deviation_penalty = min(abs(float(pi[-1]) - pi_target) * 5, 20)

        score = min(cred_penalty + vol_penalty + persistence_penalty + deviation_penalty, 100)

        return {"score": round(score, 1), "results": results}
