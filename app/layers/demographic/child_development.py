"""Child development economics: Heckman equation, Perry Preschool, nutrition.

Models the economics of early childhood investment using four frameworks:

1. Heckman equation (Heckman 2006): returns to human capital investment are
   highest in early childhood and decline with age. The rate of return to
   early intervention (preschool, nutrition, parenting) exceeds returns to
   schooling, job training, and remediation at every subsequent stage.
   Benefit-cost ratios: preschool ~7:1, K-12 ~3:1, job training ~1:1.

2. Perry Preschool long-run effects: randomized trial showing age-40 effects
   of quality preschool: +14pp employment, -20pp arrest rate, +$5,000 annual
   earnings. Internal rate of return ~7-10%. Abecedarian shows similar
   patterns with added health benefits.

3. Nutrition-cognition nexus: stunting (height-for-age z-score < -2) causes
   irreversible cognitive damage. Lancet (2007) estimates 200M children in
   developing countries fail to reach cognitive potential due to malnutrition.
   Each 1 SD increase in height-for-age z-score -> 0.2 SD increase in
   cognitive test scores.

4. Birth order effects: Black, Devereux & Salvanes (2005) find later-born
   children have lower education and earnings, consistent with resource
   dilution and declining parental investment per child.

References:
    Heckman, J.J. (2006). Skill Formation and the Economics of Investing
        in Disadvantaged Children. Science, 312(5782), 1900-1902.
    Schweinhart, L.J. et al. (2005). The High/Scope Perry Preschool Study
        Through Age 40. High/Scope Press.
    Victora, C.G. et al. (2008). Maternal and Child Undernutrition: Consequences
        for Adult Health and Human Capital. Lancet, 371(9609), 340-357.
    Black, S., Devereux, P. & Salvanes, K. (2005). The More the Merrier?
        The Effect of Family Size and Birth Order on Children's Education.
        QJE, 120(2), 669-700.

Score: high stunting + low preschool enrollment -> CRISIS, low stunting +
high early investment -> STABLE.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class ChildDevelopment(LayerBase):
    layer_id = "l17"
    name = "Child Development Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        # Prevalence of stunting (% of children under 5)
        stunting_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.STA.STNT.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Prevalence of wasting (% of children under 5)
        wasting_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.STA.WAST.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Pre-primary enrollment (% gross)
        preprimary_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.PRE.ENRR'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Government expenditure on education (% of GDP)
        edu_exp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.XPD.TOTL.GD.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Under-5 mortality rate (per 1,000 live births)
        u5mr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.DYN.MORT'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP per capita
        gdppc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Total fertility rate (for birth order / family size proxy)
        tfr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.DYN.TFRT.IN'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Human Capital Index
        hci_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'HD.HCI.OVRL'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not stunting_rows and not preprimary_rows and not u5mr_rows:
            return {"score": 50, "results": {"error": "no child development data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        stunting_data = _index(stunting_rows) if stunting_rows else {}
        wasting_data = _index(wasting_rows) if wasting_rows else {}
        prepri_data = _index(preprimary_rows) if preprimary_rows else {}
        edu_exp_data = _index(edu_exp_rows) if edu_exp_rows else {}
        u5mr_data = _index(u5mr_rows) if u5mr_rows else {}
        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}
        hci_data = _index(hci_rows) if hci_rows else {}

        # --- Heckman equation: early investment returns ---
        # Cross-country: pre-primary enrollment -> HCI relationship
        heckman = None
        prepri_list, hci_list = [], []
        for iso in set(prepri_data.keys()) & set(hci_data.keys()):
            p_yrs = prepri_data[iso]
            h_yrs = hci_data[iso]
            common = sorted(set(p_yrs.keys()) & set(h_yrs.keys()))
            if not common:
                # Use latest of each (HCI may lag enrollment data)
                p_latest = sorted(p_yrs.keys())
                h_latest = sorted(h_yrs.keys())
                if p_latest and h_latest:
                    p_val = p_yrs[p_latest[-1]]
                    h_val = h_yrs[h_latest[-1]]
                    if p_val is not None and h_val is not None and p_val >= 0:
                        prepri_list.append(p_val)
                        hci_list.append(h_val)
            else:
                yr = common[-1]
                p_val = p_yrs[yr]
                h_val = h_yrs[yr]
                if p_val is not None and h_val is not None and p_val >= 0:
                    prepri_list.append(p_val)
                    hci_list.append(h_val)

        if len(prepri_list) >= 15:
            prepri_arr = np.array(prepri_list)
            hci_arr = np.array(hci_list)
            slope, intercept, r, p, se = stats.linregress(prepri_arr, hci_arr)
            heckman = {
                "preschool_hci_elasticity": round(float(slope), 6),
                "se": round(float(se), 6),
                "r_squared": round(float(r ** 2), 4),
                "p_value": round(float(p), 6),
                "n_countries": len(prepri_list),
                "early_investment_effective": slope > 0 and p < 0.05,
                "interpretation": (
                    f"Each 1pp increase in pre-primary enrollment associated with "
                    f"{abs(slope) * 10:.4f} change in HCI per 10pp."
                ),
            }

        # --- Nutrition-cognition nexus ---
        # Stunting prevalence vs. HCI and GDP per capita
        nutrition = None
        stunt_list, outcome_list = [], []
        for iso in set(stunting_data.keys()) & set(hci_data.keys()):
            s_yrs = stunting_data[iso]
            h_yrs = hci_data[iso]
            s_latest = sorted(s_yrs.keys())
            h_latest = sorted(h_yrs.keys())
            if s_latest and h_latest:
                s_val = s_yrs[s_latest[-1]]
                h_val = h_yrs[h_latest[-1]]
                if s_val is not None and h_val is not None:
                    stunt_list.append(s_val)
                    outcome_list.append(h_val)

        if len(stunt_list) >= 15:
            stunt_arr = np.array(stunt_list)
            outcome_arr = np.array(outcome_list)
            slope, intercept, r, p, se = stats.linregress(stunt_arr, outcome_arr)
            nutrition = {
                "stunting_hci_effect": round(float(slope), 6),
                "se": round(float(se), 6),
                "r_squared": round(float(r ** 2), 4),
                "p_value": round(float(p), 6),
                "n_countries": len(stunt_list),
                "malnutrition_damages_hc": slope < 0 and p < 0.05,
                "interpretation": (
                    f"Each 1pp increase in stunting associated with "
                    f"{abs(slope):.4f} decrease in HCI."
                ),
            }

        # --- Country child development profile ---
        country_profile = None
        if country_iso3:
            profile = {}

            # Stunting
            if country_iso3 in stunting_data:
                s_c = stunting_data[country_iso3]
                yrs = sorted(s_c.keys())
                if yrs:
                    profile["stunting_pct"] = float(s_c[yrs[-1]] or 0)
                    profile["stunting_year"] = yrs[-1]
                    if len(yrs) >= 3:
                        vals = [s_c[y] for y in yrs if s_c[y] is not None]
                        if len(vals) >= 3:
                            profile["stunting_improving"] = vals[-1] < vals[0]

            # Wasting
            if country_iso3 in wasting_data:
                w_c = wasting_data[country_iso3]
                yrs = sorted(w_c.keys())
                if yrs:
                    profile["wasting_pct"] = float(w_c[yrs[-1]] or 0)

            # Pre-primary enrollment
            if country_iso3 in prepri_data:
                p_c = prepri_data[country_iso3]
                yrs = sorted(p_c.keys())
                if yrs:
                    profile["preprimary_enrollment"] = float(p_c[yrs[-1]] or 0)

            # Under-5 mortality
            if country_iso3 in u5mr_data:
                u_c = u5mr_data[country_iso3]
                yrs = sorted(u_c.keys())
                if yrs:
                    profile["under5_mortality"] = float(u_c[yrs[-1]] or 0)
                    if len(yrs) >= 5:
                        vals = [u_c[y] for y in yrs if u_c[y] is not None]
                        if len(vals) >= 5:
                            yrs_num = list(range(len(vals)))
                            slope, _, _, _, _ = stats.linregress(yrs_num, vals)
                            profile["u5mr_trend_annual"] = round(float(slope), 2)

            # Education spending
            if country_iso3 in edu_exp_data:
                e_c = edu_exp_data[country_iso3]
                yrs = sorted(e_c.keys())
                if yrs:
                    profile["education_spending_pct_gdp"] = float(e_c[yrs[-1]] or 0)

            if profile:
                country_profile = profile

        # --- Perry Preschool cost-benefit framework ---
        # Apply Heckman's estimated returns to country's investment level
        perry_analysis = None
        if country_profile and heckman:
            prepri = country_profile.get("preprimary_enrollment", 0)
            edu_spend = country_profile.get("education_spending_pct_gdp", 0)

            # Estimated cost-benefit based on enrollment gap from optimal
            optimal_prepri = 90.0  # OECD average
            enrollment_gap = max(0, optimal_prepri - prepri)

            # Heckman estimates 7-10% annual return on early investment
            # Approximate: each 10pp enrollment increase -> 7% return
            estimated_annual_return = enrollment_gap / 10 * 0.07

            perry_analysis = {
                "current_preprimary_enrollment": round(prepri, 1),
                "optimal_benchmark": optimal_prepri,
                "enrollment_gap_pp": round(enrollment_gap, 1),
                "estimated_annual_return": round(estimated_annual_return, 4),
                "education_spending_pct_gdp": round(edu_spend, 2),
                "underinvesting": prepri < 50 and edu_spend < 4,
            }

        # --- Birth order / family size effects (cross-country proxy) ---
        # Use TFR vs. HCI relationship: higher fertility -> lower per-child investment
        birth_order = None
        tfr_list, hci_bo_list = [], []
        for iso in set(hci_data.keys()) & set(
            {r["country_iso3"] for r in (tfr_rows or [])}
        ):
            tfr_yrs = _index(tfr_rows or []).get(iso, {}) if tfr_rows else {}
            hci_yrs = hci_data.get(iso, {})
            t_latest = sorted(tfr_yrs.keys()) if tfr_yrs else []
            h_latest = sorted(hci_yrs.keys()) if hci_yrs else []
            if t_latest and h_latest:
                t_val = tfr_yrs[t_latest[-1]]
                h_val = hci_yrs[h_latest[-1]]
                if t_val is not None and h_val is not None:
                    tfr_list.append(t_val)
                    hci_bo_list.append(h_val)

        if len(tfr_list) >= 15:
            tfr_arr = np.array(tfr_list)
            hci_bo_arr = np.array(hci_bo_list)
            slope, intercept, r, p, se = stats.linregress(tfr_arr, hci_bo_arr)
            birth_order = {
                "tfr_hci_elasticity": round(float(slope), 4),
                "se": round(float(se), 4),
                "r_squared": round(float(r ** 2), 4),
                "p_value": round(float(p), 6),
                "n_countries": len(tfr_list),
                "quantity_quality_tradeoff": slope < 0 and p < 0.05,
                "interpretation": (
                    f"Each additional child in TFR associated with "
                    f"{abs(slope):.4f} change in HCI."
                ),
            }

        # --- Score ---
        score = 50.0
        if country_profile:
            stunting = country_profile.get("stunting_pct", 0)
            prepri = country_profile.get("preprimary_enrollment", 0)
            u5mr = country_profile.get("under5_mortality", 0)

            # Stunting component
            if stunting > 40:
                score += 25
            elif stunting > 25:
                score += 15
            elif stunting > 10:
                score += 5
            elif stunting < 5:
                score -= 15

            # Pre-primary enrollment
            if prepri > 80:
                score -= 15
            elif prepri > 50:
                score -= 5
            elif prepri < 20:
                score += 15

            # Under-5 mortality
            if u5mr > 50:
                score += 15
            elif u5mr > 25:
                score += 5
            elif u5mr < 10:
                score -= 10

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "heckman_early_returns": heckman,
                "nutrition_cognition": nutrition,
                "perry_preschool_framework": perry_analysis,
                "birth_order_effects": birth_order,
                "country_profile": country_profile,
                "country_iso3": country_iso3,
            },
        }
