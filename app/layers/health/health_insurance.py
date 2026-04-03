"""Health insurance economics: adverse selection, moral hazard, and coverage.

Tests for adverse selection using the Chiappori-Salanie (2000) positive
correlation test. Estimates moral hazard via RAND HIE price elasticities.
Constructs a universal health coverage (UHC) index from service coverage
and financial protection indicators.

Key references:
    Chiappori, P.A. & Salanie, B. (2000). Testing for asymmetric information
        in insurance markets. JPE, 108(1), 56-78.
    Manning, W.G. et al. (1987). Health insurance and the demand for medical
        care: evidence from a randomized experiment. AER, 77(3), 251-277.
    Finkelstein, A. & McGarry, K. (2006). Multiple dimensions of private
        information: evidence from the long-term care insurance market.
        AER, 96(4), 938-958.
    WHO/World Bank (2017). Tracking universal health coverage: 2017 global
        monitoring report.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class HealthInsurance(LayerBase):
    layer_id = "l8"
    name = "Health Insurance"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate health insurance market parameters and coverage index.

        Fetches insurance coverage rates, OOP spending, health service
        coverage indicators, and health spending data. Tests for adverse
        selection, estimates moral hazard elasticities, and computes a
        universal health coverage index.

        Returns dict with score, adverse selection test, moral hazard
        estimates, RAND HIE elasticities, and UHC service coverage index.
        """
        country_iso3 = kwargs.get("country_iso3")

        # UHC service coverage index (SH.UHC.SRVS.CV.XD)
        uhc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.UHC.SRVS.CV.XD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # OOP as % of CHE
        oop_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.OOPC.CH.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Health expenditure per capita (current USD)
        hepc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.CHEX.PC.CD'
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

        # Domestic private health expenditure as % of CHE
        priv_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.PVTD.CH.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not uhc_rows and not oop_rows:
            return {"score": 50, "results": {"error": "no health insurance data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        uhc_data = _index(uhc_rows) if uhc_rows else {}
        oop_data = _index(oop_rows) if oop_rows else {}
        hepc_data = _index(hepc_rows) if hepc_rows else {}
        gdppc_data = _index(gdppc_rows) if gdppc_rows else {}
        priv_data = _index(priv_rows) if priv_rows else {}

        # --- Chiappori-Salanie adverse selection test ---
        # Cross-country proxy: test whether higher private insurance share
        # correlates with higher health spending (positive correlation = adverse selection)
        adverse_selection = None
        priv_list, hepc_list = [], []
        for iso in set(priv_data.keys()) & set(hepc_data.keys()):
            priv_years = priv_data[iso]
            hepc_years = hepc_data[iso]
            common = sorted(set(priv_years.keys()) & set(hepc_years.keys()))
            if common:
                yr = common[-1]
                p_val = priv_years[yr]
                h_val = hepc_years[yr]
                if p_val is not None and h_val is not None and h_val > 0:
                    priv_list.append(p_val)
                    hepc_list.append(np.log(h_val))

        if len(priv_list) >= 20:
            priv_arr = np.array(priv_list)
            hepc_arr = np.array(hepc_list)

            # Correlation test (Chiappori-Salanie positive correlation)
            corr, pval = stats.pearsonr(priv_arr, hepc_arr)

            # Rank correlation (more robust)
            tau, tau_pval = stats.kendalltau(priv_arr, hepc_arr)

            adverse_selection = {
                "pearson_corr": float(corr),
                "pearson_pval": float(pval),
                "kendall_tau": float(tau),
                "kendall_pval": float(tau_pval),
                "evidence_adverse_selection": bool(corr > 0 and pval < 0.05),
                "n_countries": len(priv_list),
            }

        # --- Moral hazard: RAND HIE price elasticities ---
        # Estimate the relationship between OOP share (price signal) and
        # total health spending (quantity demanded).
        # RAND HIE found elasticity ~ -0.2 (Manning et al. 1987)
        moral_hazard = None
        oop_list, spend_list, gdp_ctrl = [], [], []

        for iso in set(oop_data.keys()) & set(hepc_data.keys()) & set(gdppc_data.keys()):
            oop_years = oop_data[iso]
            hepc_years = hepc_data[iso]
            gdp_years = gdppc_data[iso]
            common = sorted(set(oop_years.keys()) & set(hepc_years.keys()) & set(gdp_years.keys()))
            if common:
                yr = common[-1]
                o_val = oop_years[yr]
                h_val = hepc_years[yr]
                g_val = gdp_years[yr]
                if o_val and o_val > 0 and h_val and h_val > 0 and g_val and g_val > 0:
                    oop_list.append(np.log(o_val))
                    spend_list.append(np.log(h_val))
                    gdp_ctrl.append(np.log(g_val))

        if len(oop_list) >= 20:
            y = np.array(spend_list)
            oop_arr = np.array(oop_list)
            gdp_arr = np.array(gdp_ctrl)

            # Bivariate: log(spending) = a + b*log(OOP_share)
            X_biv = np.column_stack([np.ones(len(oop_arr)), oop_arr])
            beta_biv, _, _, _ = np.linalg.lstsq(X_biv, y, rcond=None)

            # Controlled: log(spending) = a + b*log(OOP_share) + c*log(GDPpc)
            X_ctrl = np.column_stack([np.ones(len(oop_arr)), oop_arr, gdp_arr])
            beta_ctrl, _, _, _ = np.linalg.lstsq(X_ctrl, y, rcond=None)

            y_hat = X_ctrl @ beta_ctrl
            ss_res = np.sum((y - y_hat) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0

            # Standard errors
            n_obs = len(y)
            k = X_ctrl.shape[1]
            mse = ss_res / (n_obs - k) if n_obs > k else 0
            try:
                cov_beta = mse * np.linalg.inv(X_ctrl.T @ X_ctrl)
                se = np.sqrt(np.diag(cov_beta))
            except np.linalg.LinAlgError:
                se = np.zeros(k)

            # RAND HIE reference elasticity
            rand_hie_elasticity = -0.2  # Manning et al. (1987)

            moral_hazard = {
                "oop_spending_elasticity_bivariate": float(beta_biv[1]),
                "oop_spending_elasticity_controlled": float(beta_ctrl[1]),
                "se": float(se[1]) if len(se) > 1 else None,
                "gdppc_coef": float(beta_ctrl[2]),
                "r_squared": float(r_sq),
                "n_countries": n_obs,
                "rand_hie_reference": rand_hie_elasticity,
                "moral_hazard_consistent": bool(beta_ctrl[1] < 0),
            }

        # --- Universal health coverage index ---
        uhc_analysis = None
        if country_iso3 and country_iso3 in uhc_data:
            uhc_years = uhc_data[country_iso3]
            latest = sorted(uhc_years.keys())[-1]
            current_uhc = uhc_years[latest]

            # Trend
            uhc_trend = None
            yrs_sorted = sorted(uhc_years.keys())
            if len(yrs_sorted) >= 3:
                early = np.mean([uhc_years[y] for y in yrs_sorted[:2]])
                late = np.mean([uhc_years[y] for y in yrs_sorted[-2:]])
                span = int(yrs_sorted[-1]) - int(yrs_sorted[0])
                if span > 0:
                    uhc_trend = float((late - early) / span)

            # Cross-country percentile
            all_uhc = []
            for iso in uhc_data:
                iso_years = uhc_data[iso]
                if iso_years:
                    all_uhc.append(iso_years[sorted(iso_years.keys())[-1]])

            percentile = None
            if all_uhc:
                percentile = float(stats.percentileofscore(all_uhc, current_uhc))

            # Financial protection: impoverishing OOP spending
            oop_val = None
            if country_iso3 in oop_data:
                oop_years_c = oop_data[country_iso3]
                if latest in oop_years_c:
                    oop_val = oop_years_c[latest]

            uhc_analysis = {
                "year": latest,
                "service_coverage_index": float(current_uhc),
                "annual_trend": uhc_trend,
                "cross_country_percentile": percentile,
                "oop_share": float(oop_val) if oop_val is not None else None,
                "financial_protection_gap": bool(oop_val and oop_val > 25),
                "coverage_tier": (
                    "high" if current_uhc >= 80
                    else "medium" if current_uhc >= 60
                    else "low" if current_uhc >= 40
                    else "very_low"
                ),
            }

        # --- Score ---
        score = 40
        if uhc_analysis:
            if uhc_analysis["coverage_tier"] == "very_low":
                score += 30
            elif uhc_analysis["coverage_tier"] == "low":
                score += 20
            elif uhc_analysis["coverage_tier"] == "medium":
                score += 10
            if uhc_analysis["financial_protection_gap"]:
                score += 10

        if adverse_selection and adverse_selection["evidence_adverse_selection"]:
            score += 5

        score = float(np.clip(score, 0, 100))

        results = {
            "adverse_selection": adverse_selection,
            "moral_hazard": moral_hazard,
            "uhc_coverage": uhc_analysis,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
