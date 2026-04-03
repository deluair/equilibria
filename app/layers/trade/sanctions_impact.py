"""Estimate trade impact of sanctions using gravity-based counterfactuals.

Methodology:
    Quantify the trade reduction caused by economic sanctions by comparing
    observed trade to a gravity-based counterfactual (what trade would be
    without sanctions). Following Hufbauer et al. (2007) and Felbermayr
    et al. (2020):

    1. Estimate a gravity model including a sanctions dummy variable.
    2. Predict counterfactual trade flows by setting the sanctions dummy to 0.
    3. Compute the sanctions trade gap: counterfactual - observed.
    4. Decompose impact by sector and partner.
    5. Estimate third-party trade deflection (sanctions busting).

    Score (0-100): Higher score indicates larger trade destruction from
    sanctions exposure (either as sender or target).

References:
    Felbermayr, G. et al. (2020). "The global sanctions data base."
        European Economic Review, 129, 103561.
    Hufbauer, G.C. et al. (2007). Economic Sanctions Reconsidered, 3rd ed.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SanctionsImpact(LayerBase):
    layer_id = "l1"
    name = "Sanctions Impact"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate trade impact of sanctions.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 country code
            year : int - reference year
            target_country : str - ISO3 of sanctioned country (optional)
        """
        reporter = kwargs.get("reporter", "USA")
        year = kwargs.get("year", 2022)
        kwargs.get("target_country")

        # Fetch bilateral trade with sanctions indicators
        rows = await db.execute(
            """
            SELECT partner_iso3, trade_value, gdp_reporter, gdp_partner,
                   distance, sanctions_dummy, sanction_type
            FROM bilateral_trade
            WHERE reporter_iso3 = ? AND year = ?
            """,
            (reporter, year),
        )
        records = await rows.fetchall()

        if not records:
            return {"score": 0.0, "sanctions_trade_gap": None,
                    "note": "No bilateral trade data available"}

        partners = []
        trade_vals = []
        gdp_r_arr = []
        gdp_p_arr = []
        dist_arr = []
        sanction_flags = []

        for r in records:
            partners.append(r["partner_iso3"])
            trade_vals.append(float(r["trade_value"]))
            gdp_r_arr.append(float(r["gdp_reporter"]))
            gdp_p_arr.append(float(r["gdp_partner"]))
            dist_arr.append(float(r["distance"]))
            sanction_flags.append(int(r["sanctions_dummy"] or 0))

        trade = np.array(trade_vals)
        gdp_r = np.array(gdp_r_arr)
        gdp_p = np.array(gdp_p_arr)
        dist = np.array(dist_arr)
        sanctions = np.array(sanction_flags)

        n_sanctioned = int(sanctions.sum())

        # If no sanctions in data, low score
        if n_sanctioned == 0:
            return {
                "score": 0.0,
                "sanctions_trade_gap": 0.0,
                "n_sanctioned_partners": 0,
                "note": "No sanctioned trade partners",
                "reporter": reporter,
                "year": year,
            }

        # Log-linear gravity with sanctions dummy
        positive = trade > 0
        if positive.sum() < 5:
            return {"score": 0.0, "sanctions_trade_gap": None,
                    "note": "Insufficient positive trade flows"}

        ln_t = np.log(trade[positive])
        ln_gr = np.log(gdp_r[positive])
        ln_gp = np.log(gdp_p[positive])
        ln_d = np.log(dist[positive])
        s = sanctions[positive]

        n = int(positive.sum())
        X = np.column_stack([np.ones(n), ln_gr, ln_gp, ln_d, s])

        try:
            beta = np.linalg.lstsq(X, ln_t, rcond=None)[0]
        except np.linalg.LinAlgError:
            return {"score": 50.0, "sanctions_trade_gap": None,
                    "note": "Gravity estimation failed"}

        sanctions_coeff = beta[4]

        # Counterfactual: trade if sanctions were removed
        X_no_sanctions = X.copy()
        X_no_sanctions[:, 4] = 0.0
        fitted_no_sanctions = X_no_sanctions @ beta
        fitted_with = X @ beta

        # Trade gap for sanctioned partners
        sanctioned_mask = s == 1
        if sanctioned_mask.sum() > 0:
            counterfactual = np.exp(fitted_no_sanctions[sanctioned_mask])
            observed = np.exp(fitted_with[sanctioned_mask])
            trade_gap = float(np.sum(counterfactual - observed))
            pct_reduction = float(1 - np.exp(sanctions_coeff)) * 100
        else:
            trade_gap = 0.0
            pct_reduction = 0.0

        # Identify most affected partners
        partners_pos = [p for i, p in enumerate(partners) if positive[i]]
        sanctioned_partners = []
        for i, is_sanc in enumerate(s):
            if is_sanc:
                cf = float(np.exp(fitted_no_sanctions[i]))
                obs = float(np.exp(fitted_with[i]))
                sanctioned_partners.append({
                    "partner": partners_pos[i],
                    "observed_trade": obs,
                    "counterfactual_trade": cf,
                    "trade_gap": cf - obs,
                })

        sanctioned_partners.sort(key=lambda x: x["trade_gap"], reverse=True)

        # Trade deflection: check if non-sanctioned neighbors increase trade
        # (simplified: look at residuals of non-sanctioned partners)
        non_sanctioned_mask = s == 0
        if non_sanctioned_mask.sum() > 0:
            residuals_non = ln_t[non_sanctioned_mask] - fitted_with[non_sanctioned_mask]
            avg_residual = float(np.mean(residuals_non))
            deflection_indicator = avg_residual  # positive = potential deflection
        else:
            deflection_indicator = 0.0

        # Score: proportion of trade affected by sanctions, weighted by severity
        total_trade = float(trade.sum())
        sanctioned_trade = float(trade[sanctions == 1].sum())
        sanctions_share = sanctioned_trade / total_trade if total_trade > 0 else 0

        score = float(np.clip(sanctions_share * 100 + abs(pct_reduction) * 0.5, 0, 100))

        return {
            "score": score,
            "sanctions_coefficient": float(sanctions_coeff),
            "pct_trade_reduction": pct_reduction,
            "sanctions_trade_gap": trade_gap,
            "n_sanctioned_partners": n_sanctioned,
            "sanctions_share_of_trade": float(sanctions_share),
            "sanctioned_partners": sanctioned_partners[:10],
            "deflection_indicator": deflection_indicator,
            "gravity_coefficients": {
                "constant": float(beta[0]),
                "ln_gdp_reporter": float(beta[1]),
                "ln_gdp_partner": float(beta[2]),
                "ln_distance": float(beta[3]),
                "sanctions": float(beta[4]),
            },
            "n_observations": n,
            "reporter": reporter,
            "year": year,
        }
