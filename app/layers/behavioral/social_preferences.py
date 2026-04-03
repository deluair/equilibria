"""Social Preferences module.

Models other-regarding preferences in economic decision-making.

1. **Fehr-Schmidt Inequality Aversion** (Fehr & Schmidt 1999):
   U_i(x) = x_i - alpha * max(x_j - x_i, 0) - beta * max(x_i - x_j, 0)
   alpha >= beta >= 0: agents dislike disadvantageous inequality (alpha)
   more than advantageous inequality (beta).

2. **Bolton-Ockenfels ERC Model** (Bolton & Ockenfels 2000):
   Utility depends on own payoff and relative payoff share:
   U_i(y_i, sigma_i) where sigma_i = y_i / sum(y_j)
   Agents maximize a combination of absolute and relative payoff.

3. **Trust Game Analysis** (Berg, Dickhaut & McCabe 1995):
   Measures trust (amount sent) and trustworthiness (proportion returned)
   from data on bilateral economic relationships (trade, FDI, aid).

4. **Ultimatum Game Fairness Norms** (Guth, Schmittberger & Schwarze 1982):
   Tests minimum acceptable share thresholds. In macro data, manifested
   as resistance to unequal distributional outcomes (strikes, protests,
   policy rejection thresholds).

Score reflects social preference frictions: high inequality aversion +
low trust + strong fairness demands -> high stress score (distributional
conflict risk).

Sources: WDI (Gini, income shares, trade balances), FRED
"""

from __future__ import annotations

import numpy as np
from scipy.optimize import minimize

from app.layers.base import LayerBase


class SocialPreferences(LayerBase):
    layer_id = "l13"
    name = "Social Preferences"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # Fetch inequality data (Gini, income shares)
        gini_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SI.POV.GINI', 'GINI_INDEX')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch income share data (top 10%, bottom 40%)
        share_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN (
                  'SI.DST.10TH.10', 'SI.DST.FRST.20',
                  'SI.DST.04TH.20', 'SI.DST.05TH.20'
              )
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch bilateral relationship data (trade, FDI) for trust proxy
        bilateral_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.description
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('fred', 'wdi', 'comtrade')
              AND (ds.series_id LIKE '%TRADE%' OR ds.series_id LIKE '%FDI%'
                   OR ds.series_id LIKE '%BX.KLT%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not gini_rows and not share_rows and not bilateral_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        results = {"country": country}

        # --- 1. Fehr-Schmidt Inequality Aversion ---
        if gini_rows and len(gini_rows) >= 3:
            gini = np.array([float(r["value"]) for r in gini_rows])
            gini_dates = [r["date"] for r in gini_rows]
            fs = self._fehr_schmidt(gini)
            fs["period"] = f"{gini_dates[0]} to {gini_dates[-1]}"
            results["fehr_schmidt"] = fs
        else:
            results["fehr_schmidt"] = {"note": "insufficient Gini data"}

        # --- 2. Bolton-Ockenfels ERC ---
        if share_rows and len(share_rows) >= 3:
            shares = {}
            for r in share_rows:
                sid = r["series_id"]
                shares.setdefault(sid, []).append(float(r["value"]))
            erc = self._bolton_ockenfels(shares)
            results["bolton_ockenfels"] = erc
        else:
            results["bolton_ockenfels"] = {"note": "insufficient income share data"}

        # --- 3. Trust Game Analysis ---
        if bilateral_rows and len(bilateral_rows) >= 5:
            bilateral_vals = np.array([float(r["value"]) for r in bilateral_rows])
            trust = self._trust_analysis(bilateral_vals)
            results["trust_game"] = trust
        else:
            results["trust_game"] = {"note": "insufficient bilateral data"}

        # --- 4. Ultimatum Game Fairness ---
        if gini_rows and len(gini_rows) >= 3:
            gini = np.array([float(r["value"]) for r in gini_rows])
            fairness = self._ultimatum_fairness(gini)
            results["ultimatum_fairness"] = fairness
        else:
            results["ultimatum_fairness"] = {"note": "insufficient data"}

        # --- Score ---
        score_components = []

        # Inequality aversion penalty
        if "alpha" in results.get("fehr_schmidt", {}):
            alpha = results["fehr_schmidt"]["alpha"]
            score_components.append(min(30, alpha * 30))
        else:
            score_components.append(15)

        # Trust deficit penalty
        if "trust_index" in results.get("trust_game", {}):
            trust_idx = results["trust_game"]["trust_index"]
            score_components.append(max(0, min(25, (1 - trust_idx) * 50)))
        else:
            score_components.append(12)

        # Fairness norm tension
        if "rejection_threshold" in results.get("ultimatum_fairness", {}):
            threshold = results["ultimatum_fairness"]["rejection_threshold"]
            # Higher threshold = more demanding = more friction
            score_components.append(min(25, threshold * 50))
        else:
            score_components.append(12)

        # ERC tension
        if "relative_concern" in results.get("bolton_ockenfels", {}):
            rc = results["bolton_ockenfels"]["relative_concern"]
            score_components.append(min(20, rc * 40))
        else:
            score_components.append(10)

        score = min(100, sum(score_components))

        return {"score": round(score, 1), **results}

    @staticmethod
    def _fehr_schmidt(gini: np.ndarray) -> dict:
        """Estimate Fehr-Schmidt inequality aversion parameters.

        U_i = x_i - alpha * max(x_j - x_i, 0) - beta * max(x_i - x_j, 0)

        From macro Gini data: higher Gini and larger Gini changes
        proxy for greater inequality experience. Alpha and beta estimated
        from the asymmetric response to inequality increases vs decreases.
        """
        n = len(gini)
        gini_normalized = gini / 100  # Gini to [0, 1]

        # Changes in inequality
        changes = np.diff(gini_normalized)
        increases = changes[changes > 0]  # disadvantageous inequality growth
        decreases = changes[changes < 0]  # advantageous inequality decline

        # Alpha: sensitivity to disadvantageous inequality (increases)
        mean_increase = float(np.mean(np.abs(increases))) if len(increases) > 0 else 0
        # Beta: sensitivity to advantageous inequality (decreases)
        mean_decrease = float(np.mean(np.abs(decreases))) if len(decreases) > 0 else 0

        # Calibrate to Fehr-Schmidt ranges (alpha in [0,4], beta in [0,1])
        # Using relative magnitudes of responses
        alpha = min(4.0, mean_increase * 20 + float(np.mean(gini_normalized)) * 2)
        beta = min(alpha, min(1.0, mean_decrease * 10 + float(np.mean(gini_normalized)) * 0.5))

        # Trend in inequality
        trend = float(np.polyfit(np.arange(n), gini, 1)[0])

        return {
            "alpha": round(alpha, 4),
            "beta": round(beta, 4),
            "alpha_beta_ratio": round(alpha / max(beta, 0.01), 4),
            "mean_gini": round(float(np.mean(gini)), 2),
            "gini_trend_per_year": round(trend, 4),
            "n_inequality_increases": int(len(increases)),
            "n_inequality_decreases": int(len(decreases)),
            "reference": "Fehr & Schmidt 1999: alpha in [0,4], beta in [0, alpha]",
        }

    @staticmethod
    def _bolton_ockenfels(shares: dict[str, list[float]]) -> dict:
        """Estimate Bolton-Ockenfels ERC model parameters.

        Utility depends on own payoff and relative share:
        U(y_i, sigma_i) = a * y_i + b * (sigma_i - 1/n)^2

        Higher 'b' (relative concern) means agents care more about
        their share than their absolute payoff.
        """
        # Compute relative share dispersion from income quintile data
        all_values = []
        for sid, vals in shares.items():
            all_values.extend(vals)

        if not all_values:
            return {"relative_concern": 0.5, "note": "no share data"}

        vals = np.array(all_values)
        mean_share = float(np.mean(vals))
        fair_share = 20.0  # each quintile should be 20% under equality

        # Deviation from fair share
        deviations = vals - fair_share
        variance = float(np.var(deviations, ddof=1)) if len(deviations) > 1 else 0

        # Relative concern parameter: how far from equal shares
        relative_concern = min(1.0, variance / 100)

        return {
            "relative_concern": round(relative_concern, 4),
            "mean_share": round(mean_share, 2),
            "fair_share": fair_share,
            "share_variance": round(variance, 4),
            "reference": "Bolton & Ockenfels 2000: ERC model",
        }

    @staticmethod
    def _trust_analysis(bilateral_vals: np.ndarray) -> dict:
        """Trust game analysis from bilateral economic data.

        Berg et al. (1995): trust = proportion sent, trustworthiness =
        proportion returned. In macro data, proxied by:
        - Trust: willingness to engage in bilateral exchange (FDI, trade openness)
        - Trustworthiness: reciprocity (correlation of bilateral flows)
        """
        n = len(bilateral_vals)
        if n < 5:
            return {"trust_index": 0.5, "note": "insufficient data"}

        # Normalize to [0, 1]
        val_range = np.max(bilateral_vals) - np.min(bilateral_vals)
        if val_range > 0:
            normalized = (bilateral_vals - np.min(bilateral_vals)) / val_range
        else:
            normalized = np.full_like(bilateral_vals, 0.5)

        # Trust index: level of engagement (mean normalized value)
        trust_index = float(np.mean(normalized))

        # Reciprocity: autocorrelation (high = reciprocal relationship)
        if n >= 4:
            reciprocity = float(np.corrcoef(normalized[:-1], normalized[1:])[0, 1])
            reciprocity = max(0, reciprocity) if np.isfinite(reciprocity) else 0
        else:
            reciprocity = 0

        # Stability: inverse coefficient of variation
        cv = float(np.std(bilateral_vals, ddof=1) / max(abs(np.mean(bilateral_vals)), 1e-10))
        stability = max(0, min(1.0, 1.0 - cv))

        # Trend (growing trust vs declining)
        trend = float(np.polyfit(np.arange(n), normalized, 1)[0])

        return {
            "trust_index": round(trust_index, 4),
            "reciprocity": round(reciprocity, 4),
            "stability": round(stability, 4),
            "trend": round(trend, 6),
            "n_obs": n,
            "reference": "Berg, Dickhaut & McCabe 1995: trust game",
        }

    @staticmethod
    def _ultimatum_fairness(gini: np.ndarray) -> dict:
        """Estimate ultimatum game fairness norms.

        Guth et al. (1982): responders reject offers below a fairness
        threshold. In macro data, proxied by:
        - Rejection threshold: Gini level that triggers social unrest
        - Minimum acceptable share: implied from distributional tolerance
        """
        mean_gini = float(np.mean(gini))
        max_gini = float(np.max(gini))
        gini_std = float(np.std(gini, ddof=1)) if len(gini) > 1 else 0

        # Rejection threshold: normalized Gini (higher = more unequal)
        # Countries with high Gini that stabilizes have higher tolerance
        rejection_threshold = mean_gini / 100

        # Minimum acceptable share (from Gini to bottom share approximation)
        # Gini of G implies bottom 50% gets roughly (1-G)/2 share
        min_acceptable_share = max(0, (1 - mean_gini / 100) / 2)

        # Fairness sensitivity: Gini volatility (high vol = contested norms)
        fairness_sensitivity = min(1.0, gini_std / 5)

        return {
            "rejection_threshold": round(rejection_threshold, 4),
            "min_acceptable_share": round(min_acceptable_share, 4),
            "fairness_sensitivity": round(fairness_sensitivity, 4),
            "mean_gini": round(mean_gini, 2),
            "max_gini": round(max_gini, 2),
            "reference": "Guth, Schmittberger & Schwarze 1982: ultimatum game",
        }
