"""Digital currency: CBDC design, cross-border payments, stability, and privacy.

Methodology
-----------
1. **CBDC Design Trade-offs (Auer-Bohme 2020 Framework)**:
   Two principal axes:
     - Degree of anonymity (privacy vs. AML compliance)
     - Infrastructure: token-based vs account-based
   Design score matrix: token+privacy = high utility, account+traceable = high control.
   Financial inclusion potential: CBDC account penetration among unbanked population.
   Disintermediation risk: CBDC holdings / total bank deposits ratio.

2. **Cross-Border Payment Efficiency**:
   BIS CPMI G20 roadmap metrics (2020+):
     - Cost: cross-border transfer cost as % of transaction (target < 3%)
     - Speed: settlement time in hours (target < 1h)
     - Access: share of population with cross-border payment capability
     - Transparency: real-time tracking availability
   Composite score = 0.3*cost_score + 0.3*speed_score + 0.2*access_score + 0.2*transparency_score
   where each component is normalized to 0-1 (1 = target met).

3. **Financial Stability Implications**:
   Brunnermeier-Niepelt (2019) equivalence theorem: CBDC and bank deposits are
   equivalent if central bank lends proceeds back to banks (pass-through CBDC).
   Deposit substitution: delta_deposits = -alpha * CBDC_adoption
   Bank run acceleration: CBDC enables instant flight at zero cost (digital bank run).
   Stress test: at 10% CBDC adoption, what fraction of deposits migrate?
   Stability score = 1 - disintermediation_risk * bank_run_amplifier.

4. **Privacy-Surveillance Balance**:
   Four levels: cash-like (full privacy), pseudonymous, identified with audit,
   fully traceable. Calibrated by Westin (1967) privacy spectrum.
   Privacy score = share of transactions exempt from mandatory reporting.
   Surveillance risk index = 1 - privacy_score (high -> authoritarian risk).

References:
    Auer, R. & Bohme, R. (2020). The Technology of Retail Central Bank Digital
        Currency. BIS Quarterly Review, March 2020.
    Brunnermeier, M.K. & Niepelt, D. (2019). On the Equivalence of Private and
        Public Money. Journal of Monetary Economics 106: 27-41.
    BIS CPMI (2020). Enhancing Cross-Border Payments: Stage 3 Roadmap.
    Westin, A. (1967). Privacy and Freedom. Atheneum.

Score: high disintermediation + low payment efficiency + high surveillance -> STRESS.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class DigitalCurrency(LayerBase):
    layer_id = "l15"
    name = "Digital Currency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 5)

        series_map = {
            "cbdc_adoption": f"CBDC_ADOPTION_RATE_{country}",
            "bank_deposits": f"BANK_DEPOSITS_GDP_{country}",
            "unbanked_rate": f"UNBANKED_RATE_{country}",
            "xborder_cost": f"XBORDER_COST_PCT_{country}",
            "xborder_hours": f"XBORDER_SETTLE_HOURS_{country}",
            "privacy_score": f"CBDC_PRIVACY_SCORE_{country}",
            "deposit_change": f"BANK_DEPOSIT_CHANGE_{country}",
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

        results: dict = {"country": country, "lookback_years": lookback}
        has_any = False

        # --- 1. CBDC Design ---
        design: dict = {}
        if data.get("cbdc_adoption"):
            has_any = True
            dates = sorted(data["cbdc_adoption"])
            adoption = np.array([data["cbdc_adoption"][d] for d in dates])
            design["adoption_rate_latest"] = round(float(adoption[-1]), 4)
            design["adoption_trend"] = round(float(np.polyfit(np.arange(len(adoption)), adoption, 1)[0]), 6)

            if data.get("bank_deposits"):
                dep_dates = sorted(set(data["bank_deposits"]) & set(data["cbdc_adoption"]))
                if dep_dates:
                    dep = np.array([data["bank_deposits"][d] for d in dep_dates])
                    cbdc = np.array([data["cbdc_adoption"][d] for d in dep_dates])
                    disint_ratio = cbdc / np.maximum(dep, 1e-10)
                    design["disintermediation_ratio"] = round(float(disint_ratio[-1]), 4)
                    design["disintermediation_risk"] = (
                        "high" if disint_ratio[-1] > 0.15
                        else "moderate" if disint_ratio[-1] > 0.05
                        else "low"
                    )

        if data.get("unbanked_rate"):
            has_any = True
            ur_dates = sorted(data["unbanked_rate"])
            ur = np.array([data["unbanked_rate"][d] for d in ur_dates])
            design["unbanked_rate"] = round(float(ur[-1]), 4)
            design["inclusion_potential"] = round(float(ur[-1]), 4)

        if design:
            results["cbdc_design"] = design

        # --- 2. Cross-Border Payment Efficiency ---
        xborder: dict = {}
        if data.get("xborder_cost"):
            has_any = True
            xc_dates = sorted(data["xborder_cost"])
            xc = np.array([data["xborder_cost"][d] for d in xc_dates])
            target_cost = 3.0  # G20 target: < 3%
            cost_score = max(0.0, 1.0 - float(xc[-1]) / target_cost) if xc[-1] > 0 else 1.0
            xborder["cost_pct_latest"] = round(float(xc[-1]), 2)
            xborder["cost_score"] = round(cost_score, 4)
            xborder["meets_g20_target"] = bool(xc[-1] < target_cost)

        if data.get("xborder_hours"):
            has_any = True
            xh_dates = sorted(data["xborder_hours"])
            xh = np.array([data["xborder_hours"][d] for d in xh_dates])
            target_hours = 1.0
            speed_score = max(0.0, 1.0 - float(xh[-1]) / 48.0)  # normalize over 48h range
            xborder["settle_hours_latest"] = round(float(xh[-1]), 2)
            xborder["speed_score"] = round(speed_score, 4)
            xborder["near_instant"] = bool(xh[-1] < target_hours)

        if xborder:
            # Composite efficiency
            scores = [v for k, v in xborder.items() if k.endswith("_score")]
            if scores:
                xborder["composite_efficiency"] = round(float(np.mean(scores)), 4)
            results["cross_border_payments"] = xborder

        # --- 3. Financial Stability ---
        stability: dict = {}
        if data.get("deposit_change") and data.get("cbdc_adoption"):
            has_any = True
            common = sorted(set(data["deposit_change"]) & set(data["cbdc_adoption"]))
            if len(common) >= 4:
                dep_ch = np.array([data["deposit_change"][d] for d in common])
                cbdc_a = np.array([data["cbdc_adoption"][d] for d in common])
                if np.std(cbdc_a, ddof=1) > 1e-10:
                    corr, p_val = sp_stats.pearsonr(cbdc_a, dep_ch)
                else:
                    corr, p_val = 0.0, 1.0
                stability["deposit_cbdc_correlation"] = round(float(corr), 4)
                stability["p_value"] = round(float(p_val), 4)
                stability["substitution_significant"] = float(p_val) < 0.10 and float(corr) < 0
                # Bank run amplifier: 1 + CBDC share (instant redemption)
                if data.get("cbdc_adoption"):
                    latest_adoption = list(data["cbdc_adoption"].values())[-1]
                    bank_run_amp = 1.0 + float(latest_adoption)
                    stability["bank_run_amplifier"] = round(bank_run_amp, 4)

        if data.get("cbdc_adoption") and data.get("bank_deposits"):
            has_any = True
            dep_dates = sorted(set(data["bank_deposits"]) & set(data["cbdc_adoption"]))
            if dep_dates:
                dep_v = data["bank_deposits"][dep_dates[-1]]
                cbdc_v = data["cbdc_adoption"][dep_dates[-1]]
                # Simulate 10% CBDC shift
                simulated_dep_loss = dep_v * 0.10
                stability["sim_10pct_deposit_loss_gdp"] = round(simulated_dep_loss * 0.10, 4)
                stability["equivalence_holds"] = dep_v > 0 and cbdc_v / dep_v < 0.05

        if stability:
            results["financial_stability"] = stability

        # --- 4. Privacy-Surveillance Balance ---
        if data.get("privacy_score"):
            has_any = True
            ps_dates = sorted(data["privacy_score"])
            ps = np.array([data["privacy_score"][d] for d in ps_dates])
            surveillance_risk = 1.0 - float(ps[-1])
            results["privacy_surveillance"] = {
                "privacy_score": round(float(ps[-1]), 4),
                "surveillance_risk_index": round(surveillance_risk, 4),
                "risk_level": (
                    "high" if surveillance_risk > 0.7
                    else "moderate" if surveillance_risk > 0.4
                    else "low"
                ),
                "privacy_trend": round(float(np.polyfit(np.arange(len(ps)), ps, 1)[0]), 6),
            }

        if not has_any:
            return {"score": 50.0, "results": {"error": "no digital currency data available"}}

        # --- Score ---
        stress_score = 10.0  # baseline

        design_r = results.get("cbdc_design", {})
        if design_r.get("disintermediation_risk") == "high":
            stress_score += 25.0
        elif design_r.get("disintermediation_risk") == "moderate":
            stress_score += 12.0

        xb = results.get("cross_border_payments", {})
        if xb.get("composite_efficiency") is not None:
            eff = float(xb["composite_efficiency"])
            stress_score += (1.0 - eff) * 20.0

        stab = results.get("financial_stability", {})
        if stab.get("substitution_significant"):
            stress_score += 15.0
        if stab.get("bank_run_amplifier") is not None and float(stab["bank_run_amplifier"]) > 1.2:
            stress_score += 10.0

        priv = results.get("privacy_surveillance", {})
        if priv.get("risk_level") == "high":
            stress_score += 20.0
        elif priv.get("risk_level") == "moderate":
            stress_score += 10.0

        score = max(0.0, min(100.0, stress_score))

        return {"score": round(score, 1), "results": results}
