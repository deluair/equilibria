"""Banking sector stability analysis.

Z-score for individual bank soundness, NPL ratios and capital adequacy for
sector health, Demirguc-Kunt and Detragiache (1998, 2005) crisis prediction
model, and early warning system with multiple macro-financial indicators.

Score (0-100): based on Z-score level, NPL trend, and crisis probability.
Low Z-scores, rising NPLs, or high crisis probability push toward CRISIS.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class BankingStability(LayerBase):
    layer_id = "l7"
    name = "Banking Stability"

    # Demirguc-Kunt & Detragiache logit model coefficients (approximate)
    # From their 1998 JPE and 2005 IMF working paper
    DKD_INTERCEPT = -6.34
    DKD_COEFFICIENTS = {
        "gdp_growth": -0.10,
        "terms_of_trade_change": -0.02,
        "real_interest_rate": 0.035,
        "inflation": 0.022,
        "m2_reserves": 0.005,
        "credit_growth": 0.015,
        "deposit_insurance": 0.75,
        "law_and_order": -0.30,
    }

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fred', 'wdi', 'imf', 'banking')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.description, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no banking data"}

        # Parse into named series
        series: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            desc = (r["description"] or "").lower()
            series.setdefault(desc, []).append((r["date"], float(r["value"])))

        # Extract banking indicators
        roa = self._extract_latest(series, ["return_on_assets", "roa"])
        equity_ratio = self._extract_latest(series, ["equity_assets", "equity_ratio", "capital_ratio"])
        npl_ratio = self._extract_latest(series, ["npl", "nonperforming", "non_performing"])
        car = self._extract_latest(series, ["capital_adequacy", "car", "tier1"])
        provisions = self._extract_latest(series, ["provision", "loan_loss"])
        liquidity = self._extract_latest(series, ["liquid_assets", "liquidity_ratio"])

        # Z-score calculation
        z_score = None
        z_components = None
        roa_series = self._extract_series(series, ["return_on_assets", "roa"])
        equity_series = self._extract_series(series, ["equity_assets", "equity_ratio"])
        if roa_series and equity_series and len(roa_series) >= 3:
            roa_vals = np.array(roa_series)
            eq_val = equity_series[-1] if equity_series else 10.0
            roa_mean = float(np.mean(roa_vals))
            roa_std = float(np.std(roa_vals, ddof=1))
            if roa_std > 1e-6:
                z_score = (roa_mean + eq_val) / roa_std
            else:
                z_score = 100.0  # Very stable
            z_components = {
                "roa_mean": round(roa_mean, 4),
                "equity_ratio": round(eq_val, 4),
                "roa_std": round(roa_std, 4),
            }

        # NPL trend analysis
        npl_series = self._extract_series(series, ["npl", "nonperforming"])
        npl_trend = None
        if npl_series and len(npl_series) >= 3:
            x = np.arange(len(npl_series))
            slope, intercept, r_val, _, _ = sp_stats.linregress(x, npl_series)
            npl_trend = {
                "slope_per_year": round(float(slope), 4),
                "current": round(npl_series[-1], 2),
                "r_squared": round(float(r_val ** 2), 4),
                "direction": "rising" if slope > 0.1 else "falling" if slope < -0.1 else "stable",
            }

        # Demirguc-Kunt & Detragiache crisis prediction
        dkd_result = self._dkd_crisis_probability(series)

        # Early warning system: composite of multiple indicators
        ews = self._early_warning_system(series, z_score, npl_ratio, car)

        # Score computation
        # Z-score component: Z < 5 = crisis, Z > 30 = stable
        z_component = 50.0
        if z_score is not None:
            z_component = float(np.clip(100.0 - z_score * 2.5, 0, 100))

        # NPL component: >10% = crisis, <2% = stable
        npl_component = 30.0
        if npl_ratio is not None:
            npl_component = float(np.clip(npl_ratio * 8.0, 0, 100))

        # CAR component: <8% = crisis (Basel minimum), >15% = stable
        car_component = 30.0
        if car is not None:
            car_component = float(np.clip((15.0 - car) * 10.0, 0, 100))

        # Crisis probability component
        crisis_component = 30.0
        if dkd_result:
            crisis_component = float(np.clip(dkd_result["probability"] * 100.0, 0, 100))

        score = float(np.clip(
            0.30 * z_component + 0.25 * npl_component
            + 0.20 * car_component + 0.25 * crisis_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "z_score": {
                "value": round(z_score, 2) if z_score is not None else None,
                "components": z_components,
                "interpretation": self._z_score_interpretation(z_score),
            },
            "banking_indicators": {
                "roa_pct": round(roa, 2) if roa is not None else None,
                "npl_ratio_pct": round(npl_ratio, 2) if npl_ratio is not None else None,
                "capital_adequacy_pct": round(car, 2) if car is not None else None,
                "equity_ratio_pct": round(equity_ratio, 2) if equity_ratio is not None else None,
                "provision_ratio_pct": round(provisions, 2) if provisions is not None else None,
                "liquidity_ratio_pct": round(liquidity, 2) if liquidity is not None else None,
            },
            "npl_trend": npl_trend,
            "crisis_prediction": dkd_result,
            "early_warning": ews,
        }

    @staticmethod
    def _extract_latest(series: dict, keywords: list[str]) -> float | None:
        for key, vals in series.items():
            for kw in keywords:
                if kw in key:
                    return vals[-1][1] if vals else None
        return None

    @staticmethod
    def _extract_series(series: dict, keywords: list[str]) -> list[float] | None:
        for key, vals in series.items():
            for kw in keywords:
                if kw in key:
                    return [v[1] for v in vals] if vals else None
        return None

    def _dkd_crisis_probability(self, series: dict) -> dict | None:
        """Demirguc-Kunt & Detragiache logit model for banking crisis prediction.

        P(crisis) = 1 / (1 + exp(-Xb))
        """
        indicators = {}
        mapping = {
            "gdp_growth": ["gdp_growth", "real_gdp"],
            "terms_of_trade_change": ["terms_of_trade", "tot"],
            "real_interest_rate": ["real_interest", "lending_rate"],
            "inflation": ["inflation", "cpi_change"],
            "m2_reserves": ["m2_reserves", "m2_to_reserves"],
            "credit_growth": ["credit_growth", "domestic_credit"],
            "deposit_insurance": ["deposit_insurance"],
            "law_and_order": ["law_and_order", "rule_of_law", "governance"],
        }

        for var_name, keywords in mapping.items():
            val = self._extract_latest(series, keywords)
            if val is not None:
                indicators[var_name] = val

        if len(indicators) < 3:
            return None

        # Compute logit
        xb = self.DKD_INTERCEPT
        contributions = {}
        for var_name, coef in self.DKD_COEFFICIENTS.items():
            if var_name in indicators:
                contrib = coef * indicators[var_name]
                xb += contrib
                contributions[var_name] = round(contrib, 4)

        probability = 1.0 / (1.0 + np.exp(-xb))

        return {
            "probability": round(float(probability), 4),
            "log_odds": round(float(xb), 4),
            "indicators_used": len(indicators),
            "contributions": contributions,
            "risk_level": (
                "high" if probability > 0.3
                else "moderate" if probability > 0.1
                else "low"
            ),
        }

    @staticmethod
    def _early_warning_system(series: dict, z_score: float | None,
                               npl_ratio: float | None, car: float | None) -> dict:
        """Composite early warning system with multiple threshold indicators."""
        warnings = []
        flags = 0
        total = 0

        if z_score is not None:
            total += 1
            if z_score < 10:
                flags += 1
                warnings.append(f"Low Z-score ({z_score:.1f}): bank soundness concern")

        if npl_ratio is not None:
            total += 1
            if npl_ratio > 5.0:
                flags += 1
                warnings.append(f"High NPL ratio ({npl_ratio:.1f}%): asset quality deterioration")

        if car is not None:
            total += 1
            if car < 10.0:
                flags += 1
                warnings.append(f"Low CAR ({car:.1f}%): below regulatory comfort zone")

        # Check for rapid credit growth
        for key, vals in series.items():
            if "credit_growth" in key or "domestic_credit" in key:
                total += 1
                latest = vals[-1][1] if vals else 0
                if latest > 20:
                    flags += 1
                    warnings.append(f"Rapid credit growth ({latest:.1f}%): potential bubble")
                break

        return {
            "flags_triggered": flags,
            "indicators_checked": total,
            "alert_level": (
                "critical" if flags >= 3
                else "elevated" if flags >= 2
                else "watch" if flags >= 1
                else "normal"
            ),
            "warnings": warnings,
        }

    @staticmethod
    def _z_score_interpretation(z: float | None) -> str:
        if z is None:
            return "unavailable"
        if z > 30:
            return "very stable"
        if z > 15:
            return "stable"
        if z > 5:
            return "moderate risk"
        return "high distress risk"
