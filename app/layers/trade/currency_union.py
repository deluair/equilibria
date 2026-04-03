"""Rose effect estimation: currency union trade impact.

Methodology:
    Estimate the trade-creating effect of sharing a common currency using
    the gravity framework, following Rose (2000) and subsequent meta-analyses.

    Rose specification:
        ln(x_ij) = b0 + b1*ln(GDP_i) + b2*ln(GDP_j) - b3*ln(d_ij)
                   + b4*CU_ij + b5*controls + e_ij

    where CU_ij = 1 if i and j share a currency (e.g., Eurozone, CFA franc,
    dollarization). The Rose effect is exp(b4), the multiplicative trade
    impact of currency union membership.

    Key refinements:
    1. Control for endogeneity: countries that trade more may choose to
       share a currency. Use time-varying bilateral FE or IV.
    2. Distinguish currency union types: formal unions (EMU), dollarization,
       currency boards.
    3. Estimate trade effect relative to fixed exchange rate regime.

    Rose (2000) found ~3x effect; subsequent studies with better controls
    find 0.5x-1.5x (meta-analysis: Rose and Stanley, 2005).

    Score (0-100): Higher score indicates the country faces more exchange
    rate fragmentation (no currency union benefits, higher FX costs).

References:
    Rose, A.K. (2000). "One money, one market: the effect of common
        currencies on trade." Economic Policy, 15(30), 7-45.
    Rose, A.K. and Stanley, T.D. (2005). "A Meta-Analysis of the Effect
        of Common Currencies on International Trade." Journal of Economic
        Surveys, 19(3), 347-365.
    Glick, R. and Rose, A.K. (2002). "Does a Currency Union Affect Trade?
        The Time-Series Evidence." European Economic Review, 46(6), 1125-1151.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CurrencyUnion(LayerBase):
    layer_id = "l1"
    name = "Currency Union Effect"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate Rose effect of currency unions on trade.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 country code
            year : int - reference year
        """
        reporter = kwargs.get("reporter", "USA")
        year = kwargs.get("year", 2022)

        # Fetch bilateral trade with currency union indicator
        rows = await db.execute(
            """
            SELECT reporter_iso3, partner_iso3, trade_value,
                   gdp_reporter, gdp_partner, distance,
                   currency_union, common_language, contiguity, fta_dummy
            FROM bilateral_trade
            WHERE year = ? AND trade_value > 0
            """,
            (year,),
        )
        records = await rows.fetchall()

        if not records:
            return {"score": 50.0, "rose_effect": None,
                    "note": "No bilateral trade data available"}

        # Build arrays
        ln_trade = []
        ln_gdp_r = []
        ln_gdp_p = []
        ln_dist = []
        cu_dummy = []
        lang_dummy = []
        contig_dummy = []
        fta_dummy = []
        reporters = []
        partners_list = []

        for r in records:
            tv = float(r["trade_value"])
            gr = float(r["gdp_reporter"])
            gp = float(r["gdp_partner"])
            d = float(r["distance"])
            if tv <= 0 or gr <= 0 or gp <= 0 or d <= 0:
                continue

            ln_trade.append(np.log(tv))
            ln_gdp_r.append(np.log(gr))
            ln_gdp_p.append(np.log(gp))
            ln_dist.append(np.log(d))
            cu_dummy.append(int(r["currency_union"] or 0))
            lang_dummy.append(int(r["common_language"] or 0))
            contig_dummy.append(int(r["contiguity"] or 0))
            fta_dummy.append(int(r["fta_dummy"] or 0))
            reporters.append(r["reporter_iso3"])
            partners_list.append(r["partner_iso3"])

        n = len(ln_trade)
        if n < 20:
            return {"score": 50.0, "rose_effect": None,
                    "note": "Insufficient observations for Rose estimation"}

        y = np.array(ln_trade)

        # Full gravity specification
        X = np.column_stack([
            np.ones(n),
            np.array(ln_gdp_r),
            np.array(ln_gdp_p),
            np.array(ln_dist),
            np.array(cu_dummy, dtype=float),
            np.array(lang_dummy, dtype=float),
            np.array(contig_dummy, dtype=float),
            np.array(fta_dummy, dtype=float),
        ])

        try:
            beta = np.linalg.lstsq(X, y, rcond=None)[0]
        except np.linalg.LinAlgError:
            return {"score": 50.0, "rose_effect": None,
                    "note": "Gravity estimation failed"}

        cu_coeff = beta[4]
        rose_effect = float(np.exp(cu_coeff))

        # Standard error approximation
        residuals = y - X @ beta
        mse = float(np.sum(residuals ** 2) / (n - X.shape[1]))
        try:
            var_beta = mse * np.linalg.inv(X.T @ X)
            se_cu = float(np.sqrt(var_beta[4, 4]))
            t_stat = cu_coeff / se_cu if se_cu > 0 else 0.0
        except np.linalg.LinAlgError:
            se_cu = 0.0
            t_stat = 0.0

        # R-squared
        ss_res = float(np.sum(residuals ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Currency union statistics
        cu_arr = np.array(cu_dummy)
        n_cu_pairs = int(cu_arr.sum())
        cu_share = n_cu_pairs / n if n > 0 else 0

        # Reporter-specific analysis
        reporter_in_cu = False
        reporter_cu_partners = []
        for i in range(n):
            if reporters[i] == reporter and cu_dummy[i] == 1:
                reporter_in_cu = True
                reporter_cu_partners.append(partners_list[i])

        # Counterfactual: trade gain from currency union membership
        if reporter_in_cu:
            reporter_mask = np.array([r == reporter for r in reporters])
            cu_mask = np.array(cu_dummy) == 1
            both = reporter_mask & cu_mask
            if both.sum() > 0:
                with_cu = np.sum(np.exp(X[both] @ beta))
                X_no_cu = X[both].copy()
                X_no_cu[:, 4] = 0.0
                without_cu = np.sum(np.exp(X_no_cu @ beta))
                trade_gain = float(with_cu - without_cu)
            else:
                trade_gain = 0.0
        else:
            trade_gain = 0.0

        # Score: if not in currency union, higher score (more FX friction)
        # If in CU with strong Rose effect, lower score
        if reporter_in_cu:
            # Benefit from CU; score inversely related to Rose effect
            score = float(np.clip((1 - min(rose_effect, 3) / 3) * 50, 0, 50))
        else:
            # Not in CU; score based on potential benefit foregone
            score = float(np.clip(50 + rose_effect * 10, 50, 100))

        return {
            "score": score,
            "rose_effect": rose_effect,
            "cu_coefficient": float(cu_coeff),
            "cu_se": se_cu,
            "cu_t_stat": float(t_stat),
            "r_squared": r_squared,
            "n_observations": n,
            "n_cu_pairs": n_cu_pairs,
            "cu_share": float(cu_share),
            "reporter_in_cu": reporter_in_cu,
            "reporter_cu_partners": reporter_cu_partners,
            "trade_gain_from_cu": trade_gain,
            "gravity_coefficients": {
                "constant": float(beta[0]),
                "ln_gdp_reporter": float(beta[1]),
                "ln_gdp_partner": float(beta[2]),
                "ln_distance": float(beta[3]),
                "currency_union": float(beta[4]),
                "common_language": float(beta[5]),
                "contiguity": float(beta[6]),
                "fta": float(beta[7]),
            },
            "reporter": reporter,
            "year": year,
        }
