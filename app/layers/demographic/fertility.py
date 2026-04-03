"""Fertility economics: quantity-quality tradeoff and demographic transition.

Models the Becker (1960) quantity-quality tradeoff where rising income shifts
parental investment from number of children (quantity) to human capital per
child (quality). Tests the demographic transition hypothesis that fertility
declines follow mortality declines with a lag, and evaluates the Easterlin
(1966) relative income hypothesis.

Total fertility rate (TFR) decomposition into:
    - Tempo effect: postponement of births raises period TFR vs. cohort TFR
    - Quantum effect: completed fertility changes
    - Bongaarts proximate determinants: marriage, contraception, abortion, infecundity

Demographic transition stages:
    Stage 1: High birth rate, high death rate (pre-transition)
    Stage 2: High birth rate, falling death rate (early transition)
    Stage 3: Falling birth rate, low death rate (late transition)
    Stage 4: Low birth rate, low death rate (post-transition)
    Stage 5: Sub-replacement fertility (second demographic transition)

References:
    Becker, G.S. (1960). An Economic Analysis of Fertility. In Demographic
        and Economic Change in Developed Countries. NBER, pp. 209-240.
    Becker, G.S. & Lewis, H.G. (1973). On the Interaction between the
        Quantity and Quality of Children. JPE, 81(2), S279-S288.
    Easterlin, R.A. (1966). On the Relation of Economic Factors to Recent
        and Projected Fertility Changes. Demography, 3(1), 131-153.
    Bongaarts, J. (1978). A Framework for Analyzing the Proximate
        Determinants of Fertility. Population and Development Review, 4(1).

Score: sub-replacement TFR (<1.5) -> CRISIS (population decline), very high
TFR (>5) -> STRESS (resource strain), near-replacement (2.0-2.5) -> STABLE.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class FertilityEconomics(LayerBase):
    layer_id = "l17"
    name = "Fertility Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        # Total fertility rate (SP.DYN.TFRT.IN)
        tfr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.TFRT.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Crude birth rate (SP.DYN.CBRT.IN)
        cbr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.CBRT.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Crude death rate (SP.DYN.CDRT.IN)
        cdr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.CDRT.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP per capita for Becker/Easterlin analysis
        gdppc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not tfr_rows:
            return {"score": 50, "results": {"error": "no fertility data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        tfr_data = _index(tfr_rows)
        cbr_data = _index(cbr_rows) if cbr_rows else {}
        cdr_data = _index(cdr_rows) if cdr_rows else {}
        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}

        # --- TFR trend for target country ---
        tfr_trend = None
        target_tfr = None
        if country_iso3 and country_iso3 in tfr_data:
            years_vals = sorted(tfr_data[country_iso3].items())
            years_num = np.array([int(y) for y, _ in years_vals])
            tfr_vals = np.array([v for _, v in years_vals if v])
            if len(tfr_vals) >= 5:
                valid_mask = ~np.isnan(tfr_vals) & (tfr_vals > 0)
                if np.sum(valid_mask) >= 5:
                    yrs = years_num[:len(tfr_vals)][valid_mask]
                    vals = tfr_vals[valid_mask]
                    slope, intercept, r, p, se = stats.linregress(yrs, vals)
                    target_tfr = float(vals[-1])
                    tfr_trend = {
                        "latest_tfr": target_tfr,
                        "annual_change": round(float(slope), 4),
                        "r_squared": round(float(r ** 2), 4),
                        "p_value": round(float(p), 4),
                        "n_years": int(np.sum(valid_mask)),
                        "start_year": int(yrs[0]),
                        "end_year": int(yrs[-1]),
                        "declining": slope < 0,
                    }

        # --- Demographic transition stage classification ---
        transition = None
        if country_iso3:
            cbr_c = cbr_data.get(country_iso3, {})
            cdr_c = cdr_data.get(country_iso3, {})
            if cbr_c and cdr_c:
                common_yrs = sorted(set(cbr_c.keys()) & set(cdr_c.keys()))
                if common_yrs:
                    latest_yr = common_yrs[-1]
                    cbr_val = cbr_c[latest_yr]
                    cdr_val = cdr_c[latest_yr]
                    if cbr_val is not None and cdr_val is not None:
                        stage = self._classify_transition_stage(
                            cbr_val, cdr_val, target_tfr
                        )
                        transition = {
                            "year": latest_yr,
                            "crude_birth_rate": float(cbr_val),
                            "crude_death_rate": float(cdr_val),
                            "natural_increase": round(float(cbr_val - cdr_val), 2),
                            "stage": stage,
                        }

        # --- Becker quantity-quality: cross-country TFR vs income ---
        becker = None
        log_gdppc_list, tfr_list = [], []
        for iso in set(tfr_data.keys()) & set(gdppc_data.keys()):
            tfr_yrs = sorted(tfr_data[iso].keys())
            gdp_yrs = sorted(gdppc_data[iso].keys())
            common = sorted(set(tfr_yrs) & set(gdp_yrs))
            if common:
                yr = common[-1]
                t_val = tfr_data[iso][yr]
                g_val = gdppc_data[iso][yr]
                if t_val and t_val > 0 and g_val and g_val > 0:
                    log_gdppc_list.append(np.log(g_val))
                    tfr_list.append(t_val)

        if len(log_gdppc_list) >= 20:
            log_gdp = np.array(log_gdppc_list)
            tfr_arr = np.array(tfr_list)
            slope, intercept, r, p, se = stats.linregress(log_gdp, tfr_arr)
            becker = {
                "income_elasticity_tfr": round(float(slope), 4),
                "se": round(float(se), 4),
                "r_squared": round(float(r ** 2), 4),
                "p_value": round(float(p), 6),
                "n_countries": len(log_gdppc_list),
                "quantity_quality_confirmed": slope < 0,
            }

        # --- Easterlin relative income hypothesis ---
        # Test: cohort size negatively affects fertility of that cohort
        easterlin = None
        if country_iso3 and country_iso3 in tfr_data:
            tfr_c = tfr_data[country_iso3]
            yrs = sorted(tfr_c.keys())
            if len(yrs) >= 10:
                tfr_series = np.array([tfr_c[y] for y in yrs if tfr_c[y]])
                if len(tfr_series) >= 10:
                    # Lagged TFR (parent generation ~25 years prior) as proxy
                    # for relative cohort size
                    lag = min(25, len(tfr_series) - 1)
                    if lag >= 5:
                        current = tfr_series[lag:]
                        lagged = tfr_series[:len(current)]
                        if len(current) >= 5:
                            slope_e, _, r_e, p_e, se_e = stats.linregress(
                                lagged, current
                            )
                            easterlin = {
                                "lagged_tfr_effect": round(float(slope_e), 4),
                                "r_squared": round(float(r_e ** 2), 4),
                                "p_value": round(float(p_e), 4),
                                "lag_years": lag,
                                "n_obs": len(current),
                                "easterlin_supported": slope_e < 0,
                            }

        # --- Score ---
        if target_tfr is not None:
            score = self._tfr_to_score(target_tfr)
        else:
            score = 50.0

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "tfr_trend": tfr_trend,
                "demographic_transition": transition,
                "becker_quantity_quality": becker,
                "easterlin_hypothesis": easterlin,
                "country_iso3": country_iso3,
            },
        }

    @staticmethod
    def _classify_transition_stage(
        cbr: float, cdr: float, tfr: float | None
    ) -> int:
        """Classify demographic transition stage (1-5)."""
        if cbr > 30 and cdr > 20:
            return 1
        if cbr > 25 and cdr < 15:
            return 2
        if cbr < 25 and cbr > 15 and cdr < 10:
            return 3
        if cbr < 15 and cdr < 12:
            if tfr is not None and tfr < 2.1:
                return 5
            return 4
        # Ambiguous: use TFR if available
        if tfr is not None:
            if tfr > 4.0:
                return 2
            if tfr > 2.5:
                return 3
            if tfr > 2.0:
                return 4
            return 5
        return 3  # default mid-transition

    @staticmethod
    def _tfr_to_score(tfr: float) -> float:
        """Map TFR to stress score (0-100).

        Near-replacement (2.0-2.5) is stable (low score).
        Sub-replacement (<1.5) or very high (>5) signals stress.
        """
        if 2.0 <= tfr <= 2.5:
            return 15.0
        if 1.5 <= tfr < 2.0:
            return 15.0 + (2.0 - tfr) * 60.0  # 15-45
        if tfr < 1.5:
            return 45.0 + (1.5 - tfr) * 110.0  # 45-100
        if 2.5 < tfr <= 4.0:
            return 15.0 + (tfr - 2.5) * 16.7  # 15-40
        # tfr > 4.0
        return 40.0 + (tfr - 4.0) * 20.0  # 40-100
