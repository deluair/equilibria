"""Tariff pass-through to domestic prices.

Tariff pass-through measures what fraction of a tariff increase is reflected
in the domestic price of the imported good.  Full pass-through (beta = 1)
means the entire tariff is borne by domestic consumers.  Incomplete
pass-through (beta < 1) means foreign exporters absorb part of the tariff
by reducing their FOB price.

Standard specification (Feenstra 1989, Goldberg & Knetter 1997):
    ln(P_d) = a + beta * ln(1 + tau) + gamma * ln(P_f) + delta * ln(E) + u

where P_d = domestic price, tau = tariff rate, P_f = foreign price,
E = exchange rate.  beta is the pass-through coefficient.

Pass-through depends on:
- Market structure: monopolistic competition -> lower pass-through
- Product differentiation: differentiated products -> lower pass-through
- Market share: large exporters absorb less (pricing-to-market)
- Exchange rate regime: fixed rate amplifies tariff pass-through

Welfare implications:
- Full pass-through: tariff revenue + consumer deadweight loss
- Partial pass-through: some terms-of-trade improvement for importer
- Over-shifting (beta > 1): possible under imperfect competition

The score reflects tariff vulnerability: high pass-through means tariff
increases directly raise consumer prices (high stress).
"""

import numpy as np

from app.layers.base import LayerBase


class TariffPassthrough(LayerBase):
    layer_id = "l1"
    name = "Tariff Pass-Through"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Fetch tariff, domestic price, foreign price, and exchange rate data
        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'comtrade', 'imf', 'fred', 'trains')
              AND (ds.name LIKE '%tariff%'
                   OR ds.name LIKE '%import%price%'
                   OR ds.name LIKE '%domestic%price%'
                   OR ds.name LIKE '%consumer%price%'
                   OR ds.name LIKE '%exchange%rate%'
                   OR ds.name LIKE '%cpi%'
                   OR ds.name LIKE '%producer%price%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no tariff/price data"}

        # Organize by variable type
        tariff: dict[str, float] = {}
        domestic_price: dict[str, float] = {}
        foreign_price: dict[str, float] = {}
        exchange_rate: dict[str, float] = {}

        for row in rows:
            name = row["name"].lower()
            date = row["date"]
            val = row["value"]
            if val is None:
                continue

            if "tariff" in name:
                tariff[date] = val
            elif any(kw in name for kw in ["domestic price", "consumer price", "cpi", "producer price"]):
                domestic_price[date] = val
            elif "import" in name and "price" in name:
                foreign_price[date] = val
            elif "exchange" in name and "rate" in name:
                exchange_rate[date] = val

        # Try estimation with available data
        result = {}

        # Full specification: domestic price ~ tariff + foreign price + exchange rate
        full_est = self._estimate_passthrough(tariff, domestic_price, foreign_price, exchange_rate)
        if full_est:
            result["full_specification"] = full_est

        # Reduced form: domestic price ~ tariff only
        reduced_est = self._estimate_reduced(tariff, domestic_price)
        if reduced_est:
            result["reduced_form"] = reduced_est

        if not result:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for estimation"}

        # Use best available pass-through estimate for scoring
        pt_estimate = None
        if "full_specification" in result:
            pt_estimate = result["full_specification"]["passthrough_coefficient"]
        elif "reduced_form" in result:
            pt_estimate = result["reduced_form"]["passthrough_coefficient"]

        # Score: high pass-through = high vulnerability to tariff shocks
        if pt_estimate is not None:
            abs_pt = abs(pt_estimate)
            if abs_pt > 1.0:
                score = min(100.0, 70.0 + (abs_pt - 1.0) * 30.0)  # Over-shifting
            else:
                score = abs_pt * 70.0  # Linear mapping [0,1] -> [0,70]
        else:
            score = 50.0

        # Tariff level assessment
        tariff_assessment = None
        if tariff:
            latest_date = max(tariff.keys())
            tariff_vals = np.array(list(tariff.values()))
            tariff_assessment = {
                "latest_tariff_rate": round(tariff[latest_date], 2),
                "mean_tariff": round(float(np.mean(tariff_vals)), 2),
                "tariff_trend": (
                    "declining"
                    if len(tariff_vals) > 1 and tariff_vals[-1] < tariff_vals[0]
                    else "increasing" if len(tariff_vals) > 1 else "unknown"
                ),
            }

        score = max(0.0, min(100.0, score))

        output = {
            "score": round(score, 2),
            "country": country,
            **result,
        }

        if pt_estimate is not None:
            output["passthrough_summary"] = {
                "estimate": round(pt_estimate, 4),
                "classification": self._classify_passthrough(pt_estimate),
                "interpretation": self._interpret_passthrough(pt_estimate),
            }

        if tariff_assessment:
            output["tariff_assessment"] = tariff_assessment

        return output

    @staticmethod
    def _estimate_passthrough(
        tariff: dict[str, float],
        domestic_price: dict[str, float],
        foreign_price: dict[str, float],
        exchange_rate: dict[str, float],
    ) -> dict | None:
        """Full specification pass-through estimation."""
        if not tariff or not domestic_price:
            return None

        # Find common dates across all available series
        common = sorted(set(tariff.keys()) & set(domestic_price.keys()))
        has_foreign = bool(foreign_price)
        has_fx = bool(exchange_rate)

        if has_foreign:
            common = sorted(set(common) & set(foreign_price.keys()))
        if has_fx:
            common = sorted(set(common) & set(exchange_rate.keys()))

        if len(common) < 8:
            return None

        # Build variables (in logs where appropriate)
        ln_pd = np.array([np.log(max(domestic_price[d], 1e-10)) for d in common])
        ln_tariff = np.array([np.log(1.0 + tariff[d] / 100.0) for d in common])

        regressors = [np.ones(len(common)), ln_tariff]
        reg_names = ["constant", "ln(1+tariff)"]

        if has_foreign:
            ln_pf = np.array([np.log(max(foreign_price[d], 1e-10)) for d in common])
            regressors.append(ln_pf)
            reg_names.append("ln(foreign_price)")

        if has_fx:
            ln_e = np.array([np.log(max(exchange_rate[d], 1e-10)) for d in common])
            regressors.append(ln_e)
            reg_names.append("ln(exchange_rate)")

        X = np.column_stack(regressors)
        y = ln_pd

        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - X @ beta
        n, k = X.shape
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Standard errors
        if n > k:
            sigma2 = ss_res / (n - k)
            try:
                se = np.sqrt(sigma2 * np.diag(np.linalg.inv(X.T @ X)))
            except np.linalg.LinAlgError:
                se = np.full(k, np.nan)
        else:
            se = np.full(k, np.nan)

        return {
            "passthrough_coefficient": round(float(beta[1]), 4),
            "coefficients": {name: round(float(b), 4) for name, b in zip(reg_names, beta)},
            "std_errors": {name: round(float(s), 4) for name, s in zip(reg_names, se)},
            "r_squared": round(r2, 4),
            "n_obs": int(n),
            "date_range": [common[0], common[-1]],
        }

    @staticmethod
    def _estimate_reduced(
        tariff: dict[str, float], domestic_price: dict[str, float]
    ) -> dict | None:
        """Reduced form: domestic price changes ~ tariff changes."""
        if not tariff or not domestic_price:
            return None

        common = sorted(set(tariff.keys()) & set(domestic_price.keys()))
        if len(common) < 5:
            return None

        t_vals = np.array([tariff[d] for d in common])
        p_vals = np.array([domestic_price[d] for d in common])

        # First differences to handle non-stationarity
        if len(common) < 6:
            return None

        dt = np.diff(t_vals)
        dp = np.diff(np.log(np.maximum(p_vals, 1e-10)))

        # Simple regression: dp = a + b * dt + e
        X = np.column_stack([np.ones(len(dt)), dt])
        y = dp

        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - X @ beta
        n = len(y)
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        if n > 2:
            sigma2 = ss_res / (n - 2)
            try:
                se = np.sqrt(sigma2 * np.diag(np.linalg.inv(X.T @ X)))
            except np.linalg.LinAlgError:
                se = np.array([np.nan, np.nan])
        else:
            se = np.array([np.nan, np.nan])

        return {
            "passthrough_coefficient": round(float(beta[1]), 4),
            "std_error": round(float(se[1]), 4),
            "r_squared": round(r2, 4),
            "n_obs": int(n),
            "method": "first_differences",
        }

    @staticmethod
    def _classify_passthrough(pt: float) -> str:
        abs_pt = abs(pt)
        if abs_pt < 0.3:
            return "low pass-through"
        elif abs_pt < 0.7:
            return "moderate pass-through"
        elif abs_pt <= 1.0:
            return "high pass-through"
        else:
            return "over-shifting"

    @staticmethod
    def _interpret_passthrough(pt: float) -> str:
        abs_pt = abs(pt)
        if abs_pt < 0.3:
            return (
                "Foreign exporters absorb most of the tariff through price reductions. "
                "Domestic consumers are relatively shielded."
            )
        elif abs_pt < 0.7:
            return (
                "Tariff burden is shared between foreign exporters and domestic consumers. "
                "Partial terms-of-trade improvement for the importing country."
            )
        elif abs_pt <= 1.0:
            return (
                "Most of the tariff is passed through to domestic prices. "
                "Consumers bear the bulk of the cost."
            )
        else:
            return (
                "Over-shifting: domestic prices rise by more than the tariff. "
                "Possible under imperfect competition or markup adjustments."
            )
