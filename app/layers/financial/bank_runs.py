"""Bank run and systemic liquidity analysis: Diamond-Dybvig, deposit insurance, contagion, and LCR.

Calibrates the Diamond-Dybvig (1983) bank run model, tests deposit insurance
effectiveness, models interbank contagion networks, and computes Basel III
Liquidity Coverage Ratio compliance.

Methodology:
    1. Diamond-Dybvig model calibration:
       Bank run equilibrium exists when: c_1 * t > r_1
       where c_1 = demand-deposit payout to early withdrawers,
       t = fraction of depositors who are patient but run anyway,
       r_1 = early liquidation value of long-term assets.
       Run vulnerability index = (c_1 * n_depositors) / liquid_assets.

    2. Deposit insurance effectiveness:
       Coverage ratio = insured_deposits / total_deposits.
       Effective insurance eliminates run equilibrium when:
       c_1 * t_max <= deposit_insurance_fund + liquid_assets.
       Moral hazard cost estimated from risk-taking premium.

    3. Interbank contagion (Eisenberg-Noe 2001 clearing vector):
       min_i Pi_i = min(e_i + sum_j P_ji * Pi_j, L_i)
       where Pi_i = clearing payment, e_i = external assets,
       P_ji = interbank liabilities. Solved iteratively.
       Domino contagion: fraction of banks failing after initial shock.

    4. Liquidity Coverage Ratio (Basel III):
       LCR = HQLA / Total_net_cash_outflows_30d >= 100%
       HQLA = Level 1 (cash + central bank reserves) + 0.85*Level 2A + 0.75*Level 2B
       Net outflow = stressed outflow - min(stressed inflow, 0.75 * outflow)

    Score: run vulnerability + weak deposit insurance + contagion risk + LCR < 100% = crisis.

References:
    Diamond, D.W. & Dybvig, P.H. (1983). "Bank Runs, Deposit Insurance, and
        Liquidity." Journal of Political Economy, 91(3), 401-419.
    Eisenberg, L. & Noe, T.H. (2001). "Systemic Risk in Financial Systems."
        Management Science, 47(2), 236-249.
    Basel Committee on Banking Supervision (2013). "Basel III: The Liquidity
        Coverage Ratio and Liquidity Risk Monitoring Tools." BIS.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class BankRuns(LayerBase):
    layer_id = "l7"
    name = "Bank Runs"

    # Basel III LCR haircuts
    HQLA_HAIRCUTS = {
        "level1": 1.00,   # cash, central bank reserves, sovereigns (0% RW)
        "level2a": 0.85,  # sovereigns (20% RW), covered bonds
        "level2b": 0.75,  # equities, corporate bonds, RMBS
    }

    # Basel III stressed outflow rates
    OUTFLOW_RATES = {
        "retail_stable_deposits": 0.03,
        "retail_less_stable": 0.10,
        "unsecured_wholesale_operational": 0.25,
        "unsecured_wholesale_non_operational": 0.40,
        "secured_wholesale": 0.25,
        "committed_facilities": 0.10,
    }

    async def compute(self, db, **kwargs) -> dict:
        """Analyze bank run risk and systemic liquidity.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            lookback_years : int - data window (default 10)
            n_banks : int - number of interbank nodes for contagion sim (default 5)
        """
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)
        n_banks = kwargs.get("n_banks", 5)

        rows = await db.fetch_all(
            """
            SELECT ds.description, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('fred', 'wdi', 'imf', 'banking', 'bis')
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
        total_deposits = self._extract_series(series, ["total_deposits", "deposits"])
        insured_deposits = self._extract_series(series, ["insured_deposits", "guaranteed_deposits"])
        liquid_assets = self._extract_series(series, ["liquid_assets", "hqla", "liquidity"])
        deposit_insurance_fund = self._extract_series(series, ["deposit_insurance_fund", "dif"])
        interbank_claims = self._extract_series(series, ["interbank_claims", "interbank_assets"])
        interbank_liabilities = self._extract_series(series, ["interbank_liabilities", "interbank_funding"])
        lcr = self._extract_series(series, ["lcr", "liquidity_coverage", "liquidity_ratio"])
        deposit_growth = self._extract_series(series, ["deposit_growth", "deposit_flow"])

        # --- Diamond-Dybvig calibration ---
        dd_result = self._diamond_dybvig(
            total_deposits, liquid_assets, deposit_growth
        )

        # --- Deposit insurance effectiveness ---
        di_effectiveness = self._deposit_insurance(
            total_deposits, insured_deposits, deposit_insurance_fund, liquid_assets
        )

        # --- Interbank contagion ---
        contagion = self._interbank_contagion(
            interbank_claims, interbank_liabilities, liquid_assets, n_banks
        )

        # --- LCR compliance ---
        lcr_result = self._lcr_analysis(lcr, liquid_assets, total_deposits)

        # --- Score ---
        # Run vulnerability
        run_component = 50.0
        if dd_result and dd_result.get("run_vulnerability_index") is not None:
            rvi = dd_result["run_vulnerability_index"]
            run_component = float(np.clip(rvi * 50.0, 0, 100))

        # Deposit insurance coverage
        di_component = 50.0
        if di_effectiveness and di_effectiveness.get("coverage_ratio") is not None:
            cov = di_effectiveness["coverage_ratio"]
            di_component = float(np.clip(100.0 - cov * 100.0, 0, 100))

        # Contagion component
        ct_component = 50.0
        if contagion and contagion.get("contagion_fraction") is not None:
            ct_component = float(np.clip(contagion["contagion_fraction"] * 100.0, 0, 100))

        # LCR component: < 1.0 = stress
        lcr_component = 50.0
        if lcr_result and lcr_result.get("lcr_ratio") is not None:
            lcr_val = lcr_result["lcr_ratio"]
            lcr_component = float(np.clip((1.0 - lcr_val) * 150.0, 0, 100))

        score = float(np.clip(
            0.30 * run_component + 0.25 * di_component
            + 0.20 * ct_component + 0.25 * lcr_component,
            0, 100,
        ))

        return {
            "score": round(score, 2),
            "country": country,
            "diamond_dybvig": dd_result,
            "deposit_insurance": di_effectiveness,
            "interbank_contagion": contagion,
            "liquidity_coverage_ratio": lcr_result,
        }

    @staticmethod
    def _extract_series(series: dict, keywords: list[str]) -> list[float] | None:
        for key, vals in series.items():
            for kw in keywords:
                if kw in key:
                    return [v[1] for v in vals]
        return None

    @staticmethod
    def _diamond_dybvig(
        deposits: list[float] | None,
        liquid: list[float] | None,
        dep_growth: list[float] | None,
    ) -> dict:
        """Diamond-Dybvig bank run vulnerability calibration.

        Run equilibrium arises when c_1 * fraction > liquid_assets.
        Vulnerability index = demand_deposits / liquid_assets.
        """
        dep = float(deposits[-1]) if deposits else 100.0
        liq = float(liquid[-1]) if liquid else 15.0

        # Maturity mismatch: liquid vs total deposits
        liquidity_ratio = liq / max(dep, 1e-6)

        # Run vulnerability: fraction of deposits that could be withdrawn
        # before bank illiquidity. Standard demand-deposit payout c1 ~ 1.0
        c1 = 1.0  # normalized
        # Critical fraction of patient depositors that trigger run
        t_critical = liquidity_ratio  # liq/dep is the "safe fraction"

        rvi = 1.0 / max(liquidity_ratio, 1e-6)  # higher = more vulnerable

        # Deposit growth volatility (proxy for run risk momentum)
        dep_vol = None
        if dep_growth and len(dep_growth) >= 3:
            dep_vol = float(np.std(dep_growth, ddof=1))
            # Negative deposit growth = early signal of run
            recent_growth = float(dep_growth[-1])
        else:
            recent_growth = None

        # Run equilibrium condition
        run_equilibrium_exists = liquidity_ratio < 0.20

        return {
            "liquidity_ratio": round(float(liquidity_ratio), 4),
            "run_vulnerability_index": round(float(np.clip(rvi, 0, 10)), 4),
            "critical_withdrawal_fraction": round(float(t_critical), 4),
            "run_equilibrium_exists": run_equilibrium_exists,
            "deposit_growth_volatility": round(float(dep_vol), 4) if dep_vol is not None else None,
            "recent_deposit_growth": round(float(recent_growth), 4) if recent_growth is not None else None,
            "model": "diamond_dybvig_1983",
        }

    @staticmethod
    def _deposit_insurance(
        total_deposits: list[float] | None,
        insured_deposits: list[float] | None,
        dif: list[float] | None,
        liquid: list[float] | None,
    ) -> dict:
        """Assess deposit insurance fund adequacy and moral hazard."""
        dep = float(total_deposits[-1]) if total_deposits else 100.0
        ins = float(insured_deposits[-1]) if insured_deposits else dep * 0.60
        fund = float(dif[-1]) if dif else dep * 0.015
        liq = float(liquid[-1]) if liquid else dep * 0.15

        coverage_ratio = ins / max(dep, 1e-6)
        fund_adequacy = fund / max(ins, 1e-6)  # DIF / insured deposits
        total_backstop = (fund + liq) / max(ins, 1e-6)

        # Can the backstop cover a severe run (30% withdrawal in 30 days)?
        stress_withdrawal = 0.30 * ins
        can_withstand = (fund + liq) >= stress_withdrawal

        return {
            "coverage_ratio": round(float(coverage_ratio), 4),
            "fund_to_insured_deposits": round(float(fund_adequacy), 4),
            "total_backstop_ratio": round(float(total_backstop), 4),
            "can_withstand_30pct_run": can_withstand,
            "moral_hazard_premium": round(float(coverage_ratio * 0.05), 4),
            "fund_adequacy": (
                "adequate" if fund_adequacy > 0.015
                else "borderline" if fund_adequacy > 0.008
                else "inadequate"
            ),
        }

    @staticmethod
    def _interbank_contagion(
        claims: list[float] | None,
        liabilities: list[float] | None,
        liquid: list[float] | None,
        n_banks: int,
    ) -> dict:
        """Eisenberg-Noe clearing vector and contagion simulation.

        Simplified: model n_banks symmetric banks, shock one bank,
        propagate losses through interbank network.
        """
        liq = float(liquid[-1]) if liquid else 15.0
        ib_claims = float(claims[-1]) if claims else liq * 0.3
        ib_liab = float(liabilities[-1]) if liabilities else liq * 0.3

        # Build symmetric n_banks x n_banks interbank matrix
        # Each bank has equal bilateral exposures
        total_ib = (ib_claims + ib_liab) / 2.0
        per_bank = total_ib / max(n_banks, 1)
        per_link = per_bank / max(n_banks - 1, 1)

        # External assets and liabilities per bank
        ext_assets = liq / n_banks
        ext_liab = ext_assets * 1.1  # slight leverage

        # Shock: largest bank (bank 0) loses 30% of external assets
        shock_fraction = 0.30
        shocked_assets = ext_assets * (1 - shock_fraction)

        # Eisenberg-Noe clearing vector (iterative, n_banks nodes)
        p = np.ones(n_banks) * per_bank * (n_banks - 1)  # max obligation
        p_mat = np.full((n_banks, n_banks), per_link) - np.diag(np.full(n_banks, per_link))

        ext_arr = np.full(n_banks, ext_assets)
        ext_arr[0] = shocked_assets  # shock bank 0

        for _ in range(100):
            p_new = np.minimum(
                ext_arr + p_mat.T @ p,
                p,
            )
            if float(np.max(np.abs(p_new - p))) < 1e-6:
                break
            p = p_new

        # Count defaults: payment < max obligation
        max_obligation = per_bank * (n_banks - 1)
        defaulted = float(np.sum(p < max_obligation * 0.99))
        contagion_fraction = defaulted / n_banks

        return {
            "n_banks": n_banks,
            "interbank_exposure_per_bank": round(float(per_bank), 4),
            "shock_size_pct": round(float(shock_fraction * 100), 1),
            "banks_defaulted": int(defaulted),
            "contagion_fraction": round(float(contagion_fraction), 4),
            "model": "eisenberg_noe_2001",
            "systemic_risk": "high" if contagion_fraction > 0.4 else "moderate" if contagion_fraction > 0.2 else "low",
        }

    def _lcr_analysis(
        self,
        lcr: list[float] | None,
        liquid: list[float] | None,
        deposits: list[float] | None,
    ) -> dict:
        """Basel III Liquidity Coverage Ratio compliance analysis."""
        if lcr and len(lcr) >= 1:
            lcr_val = float(lcr[-1])
            lcr_trend = None
            if len(lcr) >= 4:
                slope, _, _, _, _ = sp_stats.linregress(np.arange(len(lcr)), lcr)
                lcr_trend = round(float(slope), 4)
            return {
                "lcr_ratio": round(float(lcr_val), 4),
                "lcr_pct": round(float(lcr_val * 100), 2),
                "compliant": lcr_val >= 1.0,
                "trend_slope": lcr_trend,
                "buffer_pct": round(float((lcr_val - 1.0) * 100), 2),
            }

        # Estimate LCR from available data
        liq = float(liquid[-1]) if liquid else None
        dep = float(deposits[-1]) if deposits else None
        if liq is None or dep is None:
            return {"lcr_ratio": None, "note": "insufficient data"}

        # HQLA proxy: liquid_assets (mostly Level 1)
        hqla = liq * self.HQLA_HAIRCUTS["level1"]
        # Net outflow proxy: 10% of retail deposits over 30 days (conservative)
        net_outflow = dep * self.OUTFLOW_RATES["retail_less_stable"]
        lcr_est = hqla / max(net_outflow, 1e-6)

        return {
            "lcr_ratio": round(float(lcr_est), 4),
            "lcr_pct": round(float(lcr_est * 100), 2),
            "compliant": lcr_est >= 1.0,
            "estimated": True,
            "hqla_proxy": round(float(hqla), 4),
            "net_outflow_proxy": round(float(net_outflow), 4),
        }
