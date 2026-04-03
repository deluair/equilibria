"""Startup economics: formation, VC cycles, survival analysis, and job creation.

Methodology
-----------
1. **Startup Formation Rates**:
   Entry rate = new firms / total firms in period t.
   Normalized by population and GDP to control for business cycle:
     entry_rate_adj = (new_firms / pop) / (GDP_growth + 1)
   Comparison to long-run average: deviation signals dynamism or stagnation.
   Decker et al. (2014, 2016): US startup rate secular decline post-2000.

2. **VC Funding Cycles**:
   Gompers-Lerner (2000) hot and cold market model: VC fundraising exhibits
   cyclicality driven by capital gains tax, liquidity, and past returns.
   Funding efficiency:
     - Capital per startup (dollars raised / deals)
     - Stage distribution: seed vs Series A/B/C+ share
   Bubble indicator: year-over-year VC growth > 50% for 3+ consecutive years.

3. **Survival Analysis of New Firms (Kaplan-Meier)**:
   Hazard rate h(t) = P(exit in [t, t+dt) | survived to t).
   Stylized facts (Dunne-Roberts-Samuelson 1988): ~50% exit by year 5, ~70% by year 10.
   Kaplan-Meier estimator:
     S(t) = product_{t_i <= t} (1 - d_i / n_i)
   where d_i = deaths at t_i, n_i = firms at risk.
   Excess exits vs benchmark signals adverse selection or demand shock.

4. **Job Creation by Firm Age**:
   Haltiwanger-Jarmin-Miranda (2013): startups (age 0-5) account for all net job
   creation; net job creation by mature firms is zero on average.
   Job creation rate = (hires - separations) / (employment_t + employment_{t-1}) / 2
   Decomposed by firm age cohort (0-1, 2-5, 6-10, 11+ years).

References:
    Decker, R., Haltiwanger, J., Jarmin, R. & Miranda, J. (2014). The Role of
        Entrepreneurship in US Job Creation and Economic Dynamism. JEP 28(3): 3-24.
    Gompers, P. & Lerner, J. (2000). Money Chasing Deals? The Impact of Fund
        Inflows on Private Equity Valuations. JFE 55(2): 281-325.
    Haltiwanger, J., Jarmin, R. & Miranda, J. (2013). Who Creates Jobs? Small
        vs. Large vs. Young. REStat 95(2): 347-361.
    Dunne, T., Roberts, M.J. & Samuelson, L. (1988). Patterns of Firm Entry and
        Exit in US Manufacturing Industries. RAND JE 19(4): 495-515.

Score: high exit hazard + low formation + weak VC -> STRESS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class StartupEconomics(LayerBase):
    layer_id = "l14"
    name = "Startup Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback_years", 10)

        series_map = {
            "entry_rate": f"STARTUP_ENTRY_RATE_{country}",
            "vc_deal_count": f"VC_DEAL_COUNT_{country}",
            "vc_capital": f"VC_CAPITAL_USD_{country}",
            "survival_1y": f"STARTUP_SURVIVAL_1Y_{country}",
            "survival_5y": f"STARTUP_SURVIVAL_5Y_{country}",
            "job_creation_young": f"JOB_CREATION_YOUNG_{country}",
            "job_creation_mature": f"JOB_CREATION_MATURE_{country}",
            "total_firms": f"TOTAL_FIRMS_{country}",
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

        # --- 1. Startup Formation Rates ---
        if data.get("entry_rate"):
            has_any = True
            dates = sorted(data["entry_rate"])
            rates = np.array([data["entry_rate"][d] for d in dates])
            mean_rate = float(np.mean(rates))
            current_rate = float(rates[-1])
            trend = float(np.polyfit(np.arange(len(rates)), rates, 1)[0])
            z_current = float((current_rate - mean_rate) / np.std(rates, ddof=1)) if np.std(rates, ddof=1) > 1e-10 else 0.0
            results["formation"] = {
                "current_entry_rate": round(current_rate, 4),
                "mean_entry_rate": round(mean_rate, 4),
                "trend_slope": round(trend, 6),
                "z_score": round(z_current, 2),
                "declining": trend < -1e-4,
                "n_obs": len(rates),
            }

        # --- 2. VC Funding Cycles ---
        if data.get("vc_capital") or data.get("vc_deal_count"):
            has_any = True
            vc_results: dict = {}

            if data.get("vc_capital"):
                cap_dates = sorted(data["vc_capital"])
                capital = np.array([data["vc_capital"][d] for d in cap_dates])
                if len(capital) >= 3:
                    yoy = np.diff(capital) / np.maximum(capital[:-1], 1e-10) * 100
                    bubble_signal = bool(np.sum(yoy > 50) >= 3)
                    vc_results["capital_latest_bn"] = round(float(capital[-1]) / 1e9, 2)
                    vc_results["capital_yoy_pct"] = round(float(yoy[-1]), 1)
                    vc_results["bubble_signal"] = bubble_signal

            if data.get("vc_deal_count") and data.get("vc_capital"):
                deal_dates = sorted(set(data["vc_deal_count"]) & set(data["vc_capital"]))
                if deal_dates:
                    deals = np.array([data["vc_deal_count"][d] for d in deal_dates])
                    cap_matched = np.array([data["vc_capital"][d] for d in deal_dates])
                    capital_per_deal = cap_matched / np.maximum(deals, 1.0)
                    vc_results["capital_per_deal_mm"] = round(float(capital_per_deal[-1]) / 1e6, 2)
                    vc_results["deal_count_latest"] = int(deals[-1])

            results["vc_cycles"] = vc_results

        # --- 3. Survival Analysis ---
        if data.get("survival_1y") or data.get("survival_5y"):
            has_any = True
            surv: dict = {}

            s1_dates = sorted(data.get("survival_1y", {}).keys())
            if s1_dates:
                s1 = np.array([data["survival_1y"][d] for d in s1_dates])
                surv["survival_1y_latest"] = round(float(s1[-1]), 4)
                surv["survival_1y_benchmark"] = 0.80
                surv["below_1y_benchmark"] = float(s1[-1]) < 0.80

            s5_dates = sorted(data.get("survival_5y", {}).keys())
            if s5_dates:
                s5 = np.array([data["survival_5y"][d] for d in s5_dates])
                surv["survival_5y_latest"] = round(float(s5[-1]), 4)
                surv["survival_5y_benchmark"] = 0.50
                surv["below_5y_benchmark"] = float(s5[-1]) < 0.50

            if s1_dates and s5_dates:
                # Implied hazard rate between year 1 and 5
                common = sorted(set(s1_dates) & set(s5_dates))
                if common:
                    s1_v = data["survival_1y"][common[-1]]
                    s5_v = data["survival_5y"][common[-1]]
                    if s1_v > 1e-10:
                        implied_h = 1.0 - (s5_v / s1_v) ** (1.0 / 4.0)
                        surv["implied_annual_hazard_yr1_5"] = round(float(implied_h), 4)

            results["survival"] = surv

        # --- 4. Job Creation by Firm Age ---
        if data.get("job_creation_young") or data.get("job_creation_mature"):
            has_any = True
            jc: dict = {}

            if data.get("job_creation_young"):
                young_dates = sorted(data["job_creation_young"])
                young = np.array([data["job_creation_young"][d] for d in young_dates])
                jc["young_firm_jc_rate_latest"] = round(float(young[-1]), 4)
                jc["young_firm_jc_mean"] = round(float(np.mean(young)), 4)

            if data.get("job_creation_mature"):
                mat_dates = sorted(data["job_creation_mature"])
                mature = np.array([data["job_creation_mature"][d] for d in mat_dates])
                jc["mature_firm_jc_rate_latest"] = round(float(mature[-1]), 4)
                jc["mature_firm_jc_mean"] = round(float(np.mean(mature)), 4)

            if data.get("job_creation_young") and data.get("job_creation_mature"):
                common_jc = sorted(set(young_dates) & set(mat_dates))
                if common_jc:
                    y_v = data["job_creation_young"][common_jc[-1]]
                    m_v = data["job_creation_mature"][common_jc[-1]]
                    jc["young_to_mature_ratio"] = round(float(y_v / m_v), 4) if abs(m_v) > 1e-10 else None
                    # HJM finding: young firms dominate net job creation
                    jc["hjm_pattern"] = y_v > m_v

            results["job_creation"] = jc

        if not has_any:
            return {"score": 50.0, "results": {"error": "no startup economics data available"}}

        # --- Score ---
        stress_score = 0.0

        formation = results.get("formation", {})
        if formation.get("z_score") is not None:
            # Below-average entry rate adds stress
            z = float(formation["z_score"])
            if z < -1.0:
                stress_score += min(abs(z) * 10.0, 25.0)
            if formation.get("declining"):
                stress_score += 10.0

        vc = results.get("vc_cycles", {})
        if vc.get("bubble_signal"):
            stress_score += 15.0
        if vc.get("capital_yoy_pct") is not None and float(vc["capital_yoy_pct"]) < -30:
            stress_score += 15.0

        surv = results.get("survival", {})
        if surv.get("below_5y_benchmark"):
            stress_score += 20.0

        jc = results.get("job_creation", {})
        if jc.get("young_to_mature_ratio") is not None and float(jc["young_to_mature_ratio"]) < 1.0:
            stress_score += 15.0

        score = max(0.0, min(100.0, stress_score))

        return {"score": round(score, 1), "results": results}
