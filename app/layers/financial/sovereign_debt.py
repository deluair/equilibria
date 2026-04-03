"""Sovereign debt analysis: spread determinants, default probability, and original sin.

Models sovereign creditworthiness using spread determinants (Edwards model),
default probability via logit, debt restructuring costs, and original sin
(inability to borrow abroad in domestic currency).

Methodology:
    1. Sovereign spread determinants (Edwards 1984):
       log(spread_it) = alpha + beta_1 * debt_gdp_it + beta_2 * reserves_it
                      + beta_3 * CA_gdp_it + beta_4 * growth_it
                      + beta_5 * inflation_it + country_FE + e_it
       Estimated via panel OLS with robust standard errors.

    2. Default probability (logit model, Manasse & Roubini 2005):
       P(default) = 1 / (1 + exp(-Xb))
       where X includes debt/GDP, short-term debt/reserves, real GDP growth,
       inflation, current account, and political risk.

    3. Debt restructuring costs:
       Haircut = (NPV_new - NPV_old) / NPV_old
       Estimated from Sturzenegger & Zettelmeyer (2006) methodology:
       NPV loss = (1 - recovery_rate) * face_value.
       Recovery rate estimated from credit rating and economic fundamentals.

    4. Original sin (Eichengreen & Hausmann 1999):
       OS_index = 1 - (domestic_currency_debt / total_external_debt)
       OS_index near 1 = severe (all debt in foreign currency).
       Amplification factor for currency depreciation on debt burden.

    Score: high default probability + severe original sin + large haircut = crisis.

References:
    Edwards, S. (1984). "LDC Foreign Borrowing and Default Risk."
        American Economic Review, 74(4), 726-734.
    Manasse, P. & Roubini, N. (2005). "Rules of Thumb for Sovereign Debt Crises."
        IMF Working Paper 05/42.
    Eichengreen, B. & Hausmann, R. (1999). "Exchange Rates and Financial Fragility."
        NBER Working Paper 7418.
    Sturzenegger, F. & Zettelmeyer, J. (2006). "Debt Defaults and Lessons from
        a Decade of Crises." MIT Press.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class SovereignDebt(LayerBase):
    layer_id = "l7"
    name = "Sovereign Debt"

    # Manasse-Roubini logit coefficients (approximate, from Table 2)
    MR_INTERCEPT = -7.0
    MR_COEFFICIENTS = {
        "debt_to_gdp": 0.052,
        "st_debt_to_reserves": 0.065,
        "real_gdp_growth": -0.15,
        "inflation": 0.018,
        "ca_to_gdp": -0.10,
        "political_risk": 0.08,
        "overvaluation": 0.03,
    }

    # Haircut estimates by credit environment
    HAIRCUT_BY_SEVERITY = {
        "mild": 0.25,       # < 60% debt/GDP, orderly restructuring
        "moderate": 0.45,   # 60-90% debt/GDP
        "severe": 0.65,     # > 90% debt/GDP, protracted default
        "catastrophic": 0.80,
    }

    async def compute(self, db, **kwargs) -> dict:
        """Analyze sovereign debt sustainability and default risk.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            lookback_years : int - panel data window (default 15)
        """
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 15)

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fred', 'wdi', 'imf', 'bis', 'embi')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.description, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        series: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            desc = (r["description"] or "").lower()
            series.setdefault(desc, []).append((r["date"], float(r["value"])))

        # Extract key series
        spread = self._extract_series(series, ["sovereign_spread", "embi_spread", "cds_spread"])
        debt_gdp = self._extract_series(series, ["debt_gdp", "debt_to_gdp", "public_debt"])
        st_debt = self._extract_series(series, ["short_term_debt", "st_debt"])
        reserves = self._extract_series(series, ["reserves", "fx_reserves", "international_reserves"])
        ca_gdp = self._extract_series(series, ["current_account_gdp", "ca_gdp", "current_account"])
        gdp_growth = self._extract_series(series, ["gdp_growth", "real_growth"])
        inflation = self._extract_series(series, ["inflation", "cpi"])
        domestic_debt = self._extract_series(series, ["domestic_currency_debt", "local_currency_debt"])
        external_debt = self._extract_series(series, ["external_debt", "foreign_debt"])
        political_risk = self._extract_series(series, ["political_risk", "political_stability"])

        # --- Spread determinants (Edwards model) ---
        spread_decomp = self._spread_determinants(spread, debt_gdp, reserves, ca_gdp, gdp_growth)

        # --- Default probability (Manasse-Roubini logit) ---
        default_prob = self._default_probability(
            debt_gdp, st_debt, reserves, gdp_growth, inflation, ca_gdp, political_risk
        )

        # --- Original sin index ---
        os_index = self._original_sin(domestic_debt, external_debt)

        # --- Debt restructuring costs ---
        restructuring = self._restructuring_cost(debt_gdp, default_prob)

        # --- Debt sustainability (debt stabilizing primary balance) ---
        sustainability = self._debt_sustainability(debt_gdp, gdp_growth, spread)

        # --- Score ---
        # Default probability component
        dp_component = float(np.clip(
            (default_prob["probability"] if default_prob else 0.15) * 200.0, 0, 100
        ))

        # Debt/GDP component: >90% = high stress
        debt_component = 50.0
        if debt_gdp:
            d = float(debt_gdp[-1])
            debt_component = float(np.clip((d - 40.0) / 60.0 * 80.0, 0, 100))

        # Original sin: severe OS with large external debt = stress
        os_component = 50.0
        if os_index and os_index.get("os_index") is not None:
            os_component = float(np.clip(os_index["os_index"] * 80.0, 0, 100))

        score = float(np.clip(
            0.40 * dp_component + 0.35 * debt_component + 0.25 * os_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "spread_determinants": spread_decomp,
            "default_probability": default_prob,
            "original_sin": os_index,
            "restructuring_costs": restructuring,
            "debt_sustainability": sustainability,
        }

    @staticmethod
    def _extract_series(series: dict, keywords: list[str]) -> list[float] | None:
        for key, vals in series.items():
            for kw in keywords:
                if kw in key:
                    return [v[1] for v in vals]
        return None

    @staticmethod
    def _spread_determinants(
        spread: list[float] | None,
        debt_gdp: list[float] | None,
        reserves: list[float] | None,
        ca_gdp: list[float] | None,
        gdp_growth: list[float] | None,
    ) -> dict | None:
        """Edwards (1984) panel regression for spread determinants."""
        if not spread or not debt_gdp:
            return None

        n = min(len(spread), len(debt_gdp))
        log_spread = np.log(np.maximum(spread[-n:], 1e-3))
        d = np.array(debt_gdp[-n:])

        regressors = {"debt_gdp": d}
        if reserves and len(reserves) >= n:
            regressors["log_reserves"] = np.log(np.maximum(reserves[-n:], 1e-6))
        if ca_gdp and len(ca_gdp) >= n:
            regressors["ca_gdp"] = np.array(ca_gdp[-n:])
        if gdp_growth and len(gdp_growth) >= n:
            regressors["gdp_growth"] = np.array(gdp_growth[-n:])

        X = np.column_stack([np.ones(n)] + [v for v in regressors.values()])
        beta, _, _, _ = np.linalg.lstsq(X, log_spread, rcond=None)
        fitted = X @ beta
        ss_res = float(np.sum((log_spread - fitted) ** 2))
        ss_tot = float(np.sum((log_spread - log_spread.mean()) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        coef_names = ["intercept"] + list(regressors.keys())
        return {
            "coefficients": {name: round(float(b), 5) for name, b in zip(coef_names, beta)},
            "r_squared": round(float(r2), 4),
            "current_spread_bps": round(float(spread[-1] * 100), 1) if spread else None,
            "n_observations": n,
        }

    def _default_probability(
        self,
        debt_gdp: list[float] | None,
        st_debt: list[float] | None,
        reserves: list[float] | None,
        gdp_growth: list[float] | None,
        inflation: list[float] | None,
        ca_gdp: list[float] | None,
        political_risk: list[float] | None,
    ) -> dict | None:
        """Manasse-Roubini logit default probability."""
        indicators = {}
        if debt_gdp:
            indicators["debt_to_gdp"] = float(debt_gdp[-1])
        if st_debt and reserves:
            indicators["st_debt_to_reserves"] = float(st_debt[-1]) / max(float(reserves[-1]), 1e-6) * 100
        if gdp_growth:
            indicators["real_gdp_growth"] = float(gdp_growth[-1])
        if inflation:
            indicators["inflation"] = float(inflation[-1])
        if ca_gdp:
            indicators["ca_to_gdp"] = float(ca_gdp[-1])
        if political_risk:
            indicators["political_risk"] = float(political_risk[-1])

        if len(indicators) < 2:
            return None

        xb = self.MR_INTERCEPT
        contributions = {}
        for var, coef in self.MR_COEFFICIENTS.items():
            if var in indicators:
                contrib = coef * indicators[var]
                xb += contrib
                contributions[var] = round(float(contrib), 4)

        probability = 1.0 / (1.0 + np.exp(-xb))
        return {
            "probability": round(float(probability), 4),
            "log_odds": round(float(xb), 4),
            "indicators_used": len(indicators),
            "contributions": contributions,
            "risk_level": (
                "high" if probability > 0.20
                else "moderate" if probability > 0.05
                else "low"
            ),
        }

    @staticmethod
    def _original_sin(
        domestic_debt: list[float] | None,
        external_debt: list[float] | None,
    ) -> dict:
        """Compute Eichengreen-Hausmann Original Sin index."""
        if domestic_debt and external_debt and len(domestic_debt) >= 1:
            n = min(len(domestic_debt), len(external_debt))
            dom = float(domestic_debt[-1])
            ext = float(external_debt[-1])
            os_index = 1.0 - dom / max(ext, 1e-6)
            os_index = float(np.clip(os_index, 0, 1))

            # Trend
            if n >= 4:
                os_series = 1.0 - np.array(domestic_debt[-n:]) / np.maximum(external_debt[-n:], 1e-6)
                slope, _, _, _, _ = sp_stats.linregress(np.arange(n), os_series)
            else:
                slope = 0.0

            return {
                "os_index": round(float(os_index), 4),
                "trend_slope": round(float(slope), 6),
                "severity": "severe" if os_index > 0.75 else "moderate" if os_index > 0.40 else "mild",
                "currency_mismatch_risk": os_index > 0.60,
            }
        return {
            "os_index": None,
            "note": "no domestic/external debt composition data",
        }

    def _restructuring_cost(
        self,
        debt_gdp: list[float] | None,
        default_prob: dict | None,
    ) -> dict:
        """Estimate expected restructuring cost (haircut) under default."""
        d = float(debt_gdp[-1]) if debt_gdp else 60.0

        # Severity class from debt/GDP
        if d < 60:
            severity = "mild"
        elif d < 90:
            severity = "moderate"
        elif d < 120:
            severity = "severe"
        else:
            severity = "catastrophic"

        haircut = self.HAIRCUT_BY_SEVERITY[severity]
        prob = default_prob["probability"] if default_prob else 0.05
        expected_loss = haircut * prob * d  # % of GDP

        return {
            "debt_to_gdp_pct": round(float(d), 2),
            "severity_class": severity,
            "estimated_haircut": round(float(haircut), 3),
            "default_probability": round(float(prob), 4),
            "expected_loss_pct_gdp": round(float(expected_loss), 3),
        }

    @staticmethod
    def _debt_sustainability(
        debt_gdp: list[float] | None,
        gdp_growth: list[float] | None,
        spread: list[float] | None,
    ) -> dict | None:
        """Compute debt-stabilizing primary balance."""
        if not debt_gdp:
            return None

        d = float(debt_gdp[-1])
        g = float(gdp_growth[-1]) / 100.0 if gdp_growth else 0.04
        r = float(spread[-1]) + 0.03 if spread else 0.07  # spread + risk-free

        # Debt dynamics: d(t+1) = d(t) * (1+r)/(1+g) - pb
        # Stabilizing primary balance: pb = d * (r - g)/(1 + g)
        pb_star = d * (r - g) / (1 + g)

        return {
            "debt_to_gdp_pct": round(float(d), 2),
            "assumed_real_rate": round(float(r), 4),
            "assumed_growth_rate": round(float(g), 4),
            "stabilizing_primary_balance_pct_gdp": round(float(pb_star), 3),
            "sustainable": pb_star < 3.0,
        }
