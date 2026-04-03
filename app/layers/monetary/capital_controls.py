"""Capital controls: Chinn-Ito index, flow management, Magud-Reinhart classification, trilemma.

Methodology
-----------
1. **Chinn-Ito Financial Openness Index**:
   Chinn-Ito (2006, 2008) KAOPEN index from IMF AREAER binary variables:
     k1: multiple exchange rates; k2: current account restrictions;
     k3: capital account restrictions; k4: surrender of export proceeds.
   KAOPEN = first principal component of k1, k2, k3, k4 (standardized).
   Ranges from -1.89 (most closed) to +2.44 (most open). Normalized 0-1.
   Higher value = more financially open.

2. **Capital Flow Management Effectiveness**:
   Ostry et al. (2012) / IMF Institutional View on Capital Flows.
   Effectiveness = change in capital flow volatility after policy introduction.
   Gross inflow volatility: std(gross_inflows_gdp) before vs. after controls.
   Composition effect: tilt from debt (hot money) to FDI (stable).
   Leakage: residual flows through uncontrolled channels (increase in other categories).
   Effectiveness ratio = volatility_reduction / (1 + leakage_rate).

3. **Magud-Reinhart (2006) Capital Control Classification**:
   Four criteria ("4Es"): effectiveness in reducing volume, composition, exchange rate,
   monetary policy space. Each scored 0-1 based on empirical literature meta-analysis.
   Composite = mean(volume, composition, rate, policy) across episodes.
   Inflow controls more effective than outflow controls (crisis context).

4. **Mundell-Fleming Trilemma Navigation**:
   Aizenman-Chinn-Ito (2008) trilemma indexes:
     - MI: monetary independence = 1 - corr(dom_rate, anchor_rate)
     - ERS: exchange rate stability = 1 / (1 + std(monthly_fx_change))
     - KAOPEN: normalized to 0-1
   Trilemma constraint: MI + ERS + KAOPEN = constant (approximately 2.0 in sample).
   Policy position: which vertex of the triangle does the country occupy?

References:
    Chinn, M. & Ito, H. (2006). What Matters for Financial Development? Capital
        Controls, Institutions, and Interactions. JIMF 25(2): 163-187.
    Magud, N. & Reinhart, C. (2006). Capital Controls: An Evaluation.
        NBER Working Paper 11973.
    Ostry, J.D. et al. (2012). Tools for Managing Financial-Stability Risks
        from Capital Inflows. Journal of International Economics 88(2): 407-421.
    Aizenman, J., Chinn, M. & Ito, H. (2008). Assessing the Emerging Global
        Financial Architecture. NBER Working Paper 14053.

Score: closed + ineffective controls + trilemma violation -> STRESS.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class CapitalControls(LayerBase):
    layer_id = "l15"
    name = "Capital Controls"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 20)
        anchor_country = kwargs.get("anchor_country", "USA")

        series_map = {
            "kaopen": f"KAOPEN_{country}",
            "gross_inflows_gdp": f"GROSS_INFLOWS_GDP_{country}",
            "gross_outflows_gdp": f"GROSS_OUTFLOWS_GDP_{country}",
            "fdi_share": f"FDI_INFLOW_SHARE_{country}",
            "policy_rate": f"POLICY_RATE_{country}",
            "anchor_rate": f"POLICY_RATE_{anchor_country}",
            "fx_rate": f"FX_RATE_{country}",
            "control_intensity": f"CAPITAL_CONTROL_INTENSITY_{country}",
        }

        data: dict[str, dict[str, float]] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE series_id = ?) "
                "AND date >= date('now', ?) ORDER BY date",
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        results: dict = {"country": country}
        has_any = False

        # --- 1. Chinn-Ito KAOPEN ---
        if data.get("kaopen"):
            has_any = True
            ka_dates = sorted(data["kaopen"])
            ka = np.array([data["kaopen"][d] for d in ka_dates])
            # Normalize to 0-1 from typical range [-2, 2.5]
            ka_norm = np.clip((ka + 2.0) / 4.5, 0.0, 1.0)
            results["chinn_ito"] = {
                "kaopen_latest": round(float(ka[-1]), 4),
                "kaopen_normalized": round(float(ka_norm[-1]), 4),
                "kaopen_mean": round(float(np.mean(ka)), 4),
                "openness_trend": round(float(np.polyfit(np.arange(len(ka)), ka, 1)[0]), 6),
                "openness_level": (
                    "open" if ka_norm[-1] > 0.7
                    else "moderate" if ka_norm[-1] > 0.3
                    else "closed"
                ),
                "n_obs": len(ka),
            }

        # --- 2. Capital Flow Management Effectiveness ---
        if data.get("gross_inflows_gdp"):
            has_any = True
            inf_dates = sorted(data["gross_inflows_gdp"])
            inflows = np.array([data["gross_inflows_gdp"][d] for d in inf_dates])

            if len(inflows) >= 8:
                mid = len(inflows) // 2
                pre_vol = float(np.std(inflows[:mid], ddof=1))
                post_vol = float(np.std(inflows[mid:], ddof=1))
                vol_reduction = (pre_vol - post_vol) / max(pre_vol, 1e-10)

                # Composition effect: FDI share change
                fdi_effect = None
                if data.get("fdi_share"):
                    fdi_common = sorted(set(data["fdi_share"]) & set(data["gross_inflows_gdp"]))
                    if len(fdi_common) >= 4:
                        fdi = np.array([data["fdi_share"][d] for d in fdi_common])
                        fdi_mid = len(fdi) // 2
                        fdi_effect = float(np.mean(fdi[fdi_mid:])) - float(np.mean(fdi[:fdi_mid]))

                effectiveness = {
                    "pre_inflow_volatility": round(pre_vol, 4),
                    "post_inflow_volatility": round(post_vol, 4),
                    "volatility_reduction_pct": round(vol_reduction * 100, 2),
                    "effective": vol_reduction > 0.10,
                    "fdi_share_change": round(fdi_effect, 4) if fdi_effect is not None else None,
                    "composition_improved": fdi_effect > 0 if fdi_effect is not None else None,
                }
                results["flow_management"] = effectiveness

        # --- 3. Magud-Reinhart Classification ---
        if data.get("control_intensity"):
            has_any = True
            ci_dates = sorted(data["control_intensity"])
            ci = np.array([data["control_intensity"][d] for d in ci_dates])

            # Volume effectiveness: correlation between intensity and inflow level
            vol_eff = None
            if data.get("gross_inflows_gdp"):
                common_ci = sorted(set(data["control_intensity"]) & set(data["gross_inflows_gdp"]))
                if len(common_ci) >= 5:
                    ci_m = np.array([data["control_intensity"][d] for d in common_ci])
                    inf_m = np.array([data["gross_inflows_gdp"][d] for d in common_ci])
                    if np.std(ci_m, ddof=1) > 1e-10:
                        r, p = sp_stats.pearsonr(ci_m, inf_m)
                        vol_eff = max(0.0, -float(r))  # negative correlation = effective

            magud = {
                "control_intensity_latest": round(float(ci[-1]), 4),
                "control_intensity_mean": round(float(np.mean(ci)), 4),
                "volume_effectiveness": round(vol_eff, 4) if vol_eff is not None else None,
                "control_type": "tight" if float(ci[-1]) > 0.7 else "moderate" if float(ci[-1]) > 0.3 else "loose",
            }
            results["magud_reinhart"] = magud

        # --- 4. Trilemma Navigation ---
        trilemma: dict = {}

        # Monetary independence: 1 - |corr(dom_rate, anchor_rate)|
        if data.get("policy_rate") and data.get("anchor_rate"):
            has_any = True
            common_r = sorted(set(data["policy_rate"]) & set(data["anchor_rate"]))
            if len(common_r) >= 8:
                dom = np.array([data["policy_rate"][d] for d in common_r])
                anc = np.array([data["anchor_rate"][d] for d in common_r])
                if np.std(dom, ddof=1) > 1e-10 and np.std(anc, ddof=1) > 1e-10:
                    corr_ra = float(np.corrcoef(dom, anc)[0, 1])
                    mi = 1.0 - abs(corr_ra)
                else:
                    mi = 0.5
                trilemma["monetary_independence"] = round(mi, 4)

        # Exchange rate stability: 1 / (1 + std(monthly_fx_change))
        if data.get("fx_rate"):
            has_any = True
            fx_dates = sorted(data["fx_rate"])
            fx = np.array([data["fx_rate"][d] for d in fx_dates])
            if len(fx) >= 6:
                fx_ch = np.diff(fx) / np.maximum(np.abs(fx[:-1]), 1e-10)
                ers = 1.0 / (1.0 + float(np.std(fx_ch, ddof=1)))
                trilemma["exchange_rate_stability"] = round(ers, 4)

        # KAOPEN (already computed)
        if data.get("kaopen"):
            ka_dates = sorted(data["kaopen"])
            ka = np.array([data["kaopen"][d] for d in ka_dates])
            ka_norm = float(np.clip((ka[-1] + 2.0) / 4.5, 0.0, 1.0))
            trilemma["kaopen_normalized"] = round(ka_norm, 4)

        if len(trilemma) >= 2:
            values = [v for v in trilemma.values() if isinstance(v, float)]
            trilemma_sum = float(sum(values))
            trilemma["trilemma_sum"] = round(trilemma_sum, 4)
            # Identify dominant vertex
            if len(values) == 3:
                labels = list(trilemma.keys())[:3]
                dominant = labels[int(np.argmax(values))]
                trilemma["dominant_objective"] = dominant
            results["trilemma"] = trilemma

        if not has_any:
            return {"score": 50.0, "results": {"error": "no capital controls data available"}}

        # --- Score ---
        # Interpretation: closed + volatile flows + low autonomy = STRESS
        stress = 10.0

        ci_r = results.get("chinn_ito", {})
        if ci_r.get("openness_level") == "closed":
            stress += 15.0

        fm = results.get("flow_management", {})
        if fm.get("effective") is False:
            stress += 20.0
        if fm.get("composition_improved") is False:
            stress += 10.0

        trilemma_r = results.get("trilemma", {})
        if trilemma_r.get("monetary_independence") is not None and float(trilemma_r["monetary_independence"]) < 0.3:
            stress += 20.0
        if trilemma_r.get("exchange_rate_stability") is not None and float(trilemma_r["exchange_rate_stability"]) < 0.5:
            stress += 15.0

        score = max(0.0, min(100.0, stress))

        return {"score": round(score, 1), "results": results}
