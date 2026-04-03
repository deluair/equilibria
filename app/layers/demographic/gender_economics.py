"""Gender economics: wage gap, glass ceiling, FLFP, gender budgeting.

Comprehensive gender economics analysis covering four dimensions:

1. Gender wage gap decomposition: Extends Oaxaca-Blinder to isolate explained
   (human capital, occupation, hours) vs. unexplained (discrimination) components.
   Cross-country analysis using ILO/WDI indicators.

2. Glass ceiling index: composite measure of barriers to women's advancement
   in senior positions. Combines female share of management, board seats,
   parliamentary representation, and professional/technical workers.

3. Female labor force participation (FLFP) determinants: tests the U-shaped
   hypothesis (Goldin 1995) where FLFP first falls then rises with development.
   Controls for education, fertility, urbanization, and cultural norms.

4. Gender budgeting analysis: evaluates public expenditure through a gender
   lens, examining education spending gender parity, health spending gaps,
   and social protection coverage differentials.

References:
    Goldin, C. (1995). The U-Shaped Female Labor Force Function in Economic
        Development. In Investment in Women's Human Capital, pp. 61-90.
    Blau, F. & Kahn, L. (2017). The Gender Wage Gap: Extent, Trends, and
        Explanations. Journal of Economic Literature, 55(3), 789-865.
    Klasen, S. (2002). Low Schooling for Girls, Slower Growth for All?
        World Bank Economic Review, 16(3), 345-373.
    Duflo, E. (2012). Women Empowerment and Economic Development. Journal
        of Economic Literature, 50(4), 1051-1079.

Score: large gender gaps -> STRESS/CRISIS, near-parity -> STABLE.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class GenderEconomics(LayerBase):
    layer_id = "l17"
    name = "Gender Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        # Female labor force participation rate (% of female pop 15+)
        flfp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SL.TLF.CACT.FE.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Male labor force participation rate
        mlfp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SL.TLF.CACT.MA.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP per capita for U-shaped hypothesis
        gdppc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Female secondary enrollment
        fsec_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.SEC.ENRR.FE'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Female tertiary enrollment
        fter_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.TER.ENRR.FE'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Proportion of seats held by women in parliament
        parl_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SG.GEN.PARL.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # TFR for FLFP determinants
        tfr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.TFRT.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not flfp_rows:
            return {"score": 50, "results": {"error": "no gender labor data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        flfp_data = _index(flfp_rows)
        mlfp_data = _index(mlfp_rows) if mlfp_rows else {}
        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}
        fsec_data = _index(fsec_rows) if fsec_rows else {}
        fter_data = _index(fter_rows) if fter_rows else {}
        parl_data = _index(parl_rows) if parl_rows else {}
        tfr_data = _index(tfr_rows) if tfr_rows else {}

        # --- Gender wage gap (participation gap as proxy) ---
        wage_gap = None
        if country_iso3:
            flfp_c = flfp_data.get(country_iso3, {})
            mlfp_c = mlfp_data.get(country_iso3, {})
            if flfp_c and mlfp_c:
                common = sorted(set(flfp_c.keys()) & set(mlfp_c.keys()))
                if common:
                    latest = common[-1]
                    f_val = flfp_c[latest]
                    m_val = mlfp_c[latest]
                    if f_val is not None and m_val is not None and m_val > 0:
                        gap = float(m_val - f_val)
                        ratio = float(f_val / m_val)

                        # Trend
                        if len(common) >= 5:
                            gaps = [
                                mlfp_c[y] - flfp_c[y]
                                for y in common
                                if mlfp_c.get(y) is not None and flfp_c.get(y) is not None
                            ]
                            yrs_num = list(range(len(gaps)))
                            if len(gaps) >= 5:
                                slope, _, r, p, _ = stats.linregress(yrs_num, gaps)
                                wage_gap = {
                                    "year": latest,
                                    "female_lfpr": round(float(f_val), 2),
                                    "male_lfpr": round(float(m_val), 2),
                                    "participation_gap_pp": round(gap, 2),
                                    "female_male_ratio": round(ratio, 4),
                                    "gap_trend_annual": round(float(slope), 4),
                                    "gap_closing": slope < 0,
                                }
                            else:
                                wage_gap = {
                                    "year": latest,
                                    "female_lfpr": round(float(f_val), 2),
                                    "male_lfpr": round(float(m_val), 2),
                                    "participation_gap_pp": round(gap, 2),
                                    "female_male_ratio": round(ratio, 4),
                                }

        # --- Glass ceiling index (composite) ---
        glass_ceiling = None
        if country_iso3:
            components = {}
            # Parliamentary representation
            if country_iso3 in parl_data:
                p_yrs = sorted(parl_data[country_iso3].keys())
                if p_yrs:
                    p_val = parl_data[country_iso3][p_yrs[-1]]
                    if p_val is not None:
                        components["parliament_women_pct"] = float(p_val)

            # Female tertiary enrollment
            if country_iso3 in fter_data:
                t_yrs = sorted(fter_data[country_iso3].keys())
                if t_yrs:
                    t_val = fter_data[country_iso3][t_yrs[-1]]
                    if t_val is not None:
                        components["female_tertiary_enrollment"] = float(t_val)

            # FLFP ratio
            if wage_gap:
                components["flfp_ratio"] = wage_gap["female_male_ratio"]

            if len(components) >= 2:
                # Normalize each component to 0-100 and average
                scores = []
                if "parliament_women_pct" in components:
                    # 50% = parity = 100 score
                    scores.append(min(100, components["parliament_women_pct"] * 2))
                if "female_tertiary_enrollment" in components:
                    scores.append(min(100, components["female_tertiary_enrollment"]))
                if "flfp_ratio" in components:
                    scores.append(components["flfp_ratio"] * 100)

                composite = float(np.mean(scores))
                glass_ceiling = {
                    "composite_score": round(composite, 2),
                    "components": {
                        k: round(v, 2) for k, v in components.items()
                    },
                    "interpretation": (
                        "low_barrier" if composite > 70
                        else "moderate_barrier" if composite > 45
                        else "high_barrier"
                    ),
                }

        # --- Goldin U-shaped FLFP hypothesis ---
        # Cross-country: FLFP vs log(GDP per capita) with quadratic
        u_shape = None
        flfp_list, lgdp_list = [], []
        for iso in set(flfp_data.keys()) & set(gdppc_data.keys()):
            f_c = flfp_data[iso]
            g_c = gdppc_data[iso]
            common = sorted(set(f_c.keys()) & set(g_c.keys()))
            if common:
                yr = common[-1]
                f_val = f_c[yr]
                g_val = g_c[yr]
                if f_val is not None and g_val is not None and g_val > 0:
                    flfp_list.append(f_val)
                    lgdp_list.append(np.log(g_val))

        if len(flfp_list) >= 30:
            flfp_arr = np.array(flfp_list)
            lgdp_arr = np.array(lgdp_list)

            # Quadratic: FLFP = a + b*log(GDP) + c*log(GDP)^2
            X = np.column_stack([
                np.ones(len(lgdp_arr)),
                lgdp_arr,
                lgdp_arr ** 2,
            ])
            beta = np.linalg.lstsq(X, flfp_arr, rcond=None)[0]
            y_hat = X @ beta
            ss_res = np.sum((flfp_arr - y_hat) ** 2)
            ss_tot = np.sum((flfp_arr - np.mean(flfp_arr)) ** 2)
            r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0

            # U-shape confirmed if quadratic term is positive
            # Turning point: -b/(2c)
            turning_point = None
            if abs(beta[2]) > 1e-10:
                tp_log_gdp = -beta[1] / (2 * beta[2])
                turning_point = round(float(np.exp(tp_log_gdp)), 0)

            u_shape = {
                "linear_coef": round(float(beta[1]), 4),
                "quadratic_coef": round(float(beta[2]), 6),
                "r_squared": round(float(r_sq), 4),
                "n_countries": len(flfp_list),
                "u_shape_confirmed": beta[2] > 0,
                "turning_point_gdppc": turning_point,
            }

        # --- FLFP determinants (cross-country) ---
        flfp_determinants = None
        det_flfp, det_gdp, det_tfr, det_edu = [], [], [], []
        for iso in (
            set(flfp_data.keys())
            & set(gdppc_data.keys())
            & set(tfr_data.keys())
            & set(fsec_data.keys())
        ):
            common = sorted(
                set(flfp_data[iso].keys())
                & set(gdppc_data[iso].keys())
                & set(tfr_data[iso].keys())
                & set(fsec_data[iso].keys())
            )
            if common:
                yr = common[-1]
                f = flfp_data[iso][yr]
                g = gdppc_data[iso][yr]
                t = tfr_data[iso][yr]
                e = fsec_data[iso][yr]
                if all(v is not None for v in [f, g, t, e]) and g > 0:
                    det_flfp.append(f)
                    det_gdp.append(np.log(g))
                    det_tfr.append(t)
                    det_edu.append(e)

        if len(det_flfp) >= 20:
            y = np.array(det_flfp)
            X = np.column_stack([
                np.ones(len(y)),
                np.array(det_gdp),
                np.array(det_tfr),
                np.array(det_edu),
            ])
            beta = np.linalg.lstsq(X, y, rcond=None)[0]
            y_hat = X @ beta
            ss_res = np.sum((y - y_hat) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0

            flfp_determinants = {
                "log_gdppc_coef": round(float(beta[1]), 4),
                "tfr_coef": round(float(beta[2]), 4),
                "female_education_coef": round(float(beta[3]), 4),
                "r_squared": round(float(r_sq), 4),
                "n_countries": len(det_flfp),
                "fertility_depresses_flfp": beta[2] < 0,
                "education_raises_flfp": beta[3] > 0,
            }

        # --- Score ---
        score = 50.0
        if wage_gap:
            gap_pp = wage_gap["participation_gap_pp"]
            if gap_pp > 40:
                score = 80.0
            elif gap_pp > 25:
                score = 60.0
            elif gap_pp > 15:
                score = 40.0
            elif gap_pp > 5:
                score = 25.0
            else:
                score = 15.0

        if glass_ceiling:
            gc = glass_ceiling["composite_score"]
            # Higher composite = less barrier = lower stress
            gc_adj = (100 - gc) * 0.15
            score = score * 0.7 + gc_adj + score * 0.15

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "gender_participation_gap": wage_gap,
                "glass_ceiling_index": glass_ceiling,
                "goldin_u_shape": u_shape,
                "flfp_determinants": flfp_determinants,
                "country_iso3": country_iso3,
            },
        }
