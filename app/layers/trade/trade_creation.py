"""Trade creation and diversion analysis using gravity residuals.

Methodology:
    Estimate a gravity model of bilateral trade, then compute counterfactual
    trade flows under different trade agreement scenarios. Trade creation
    occurs when an FTA increases total trade (new flows from efficient
    partners), while trade diversion occurs when imports shift from efficient
    non-members to less efficient members.

    Following Viner (1950) and extended by Magee (2008), we:
    1. Estimate gravity equation with FTA dummies (PPML or OLS).
    2. Predict trade with and without FTA in force.
    3. Decompose the FTA effect into creation (new trade) and diversion
       (trade redirected from non-members).

    Score (0-100): Higher score indicates larger net trade diversion
    relative to creation, signaling distortionary trade policy.

References:
    Viner, J. (1950). The Customs Union Issue.
    Magee, C. (2008). "New measures of trade creation and trade diversion."
        Journal of International Economics, 75(2), 349-362.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TradeCreation(LayerBase):
    layer_id = "l1"
    name = "Trade Creation & Diversion"

    async def compute(self, db, **kwargs) -> dict:
        """Compute trade creation and diversion from gravity residuals.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 reporter country code
            partner : str - ISO3 partner country code (optional)
            fta_pair : tuple[str, str] - FTA country pair to evaluate
            year : int - reference year
        """
        reporter = kwargs.get("reporter", "USA")
        year = kwargs.get("year", 2022)

        # Fetch bilateral trade flows
        rows = await db.execute(
            """
            SELECT partner_iso3, trade_value, gdp_reporter, gdp_partner,
                   distance, contiguity, common_language, fta_dummy
            FROM bilateral_trade
            WHERE reporter_iso3 = ? AND year = ?
            """,
            (reporter, year),
        )
        records = await rows.fetchall()

        if not records:
            return {"score": 50.0, "creation": None, "diversion": None,
                    "note": "No bilateral trade data available"}

        partners = []
        trade_vals = []
        gdp_r = []
        gdp_p = []
        dists = []
        fta_flags = []

        for r in records:
            partners.append(r["partner_iso3"])
            trade_vals.append(float(r["trade_value"]))
            gdp_r.append(float(r["gdp_reporter"]))
            gdp_p.append(float(r["gdp_partner"]))
            dists.append(float(r["distance"]))
            fta_flags.append(int(r["fta_dummy"]))

        trade_arr = np.array(trade_vals)
        gdp_r_arr = np.array(gdp_r)
        gdp_p_arr = np.array(gdp_p)
        dist_arr = np.array(dists)
        fta_arr = np.array(fta_flags)

        # Log-linear gravity estimation (OLS on log-transformed data)
        # ln(T_ij) = a0 + a1*ln(GDP_i) + a2*ln(GDP_j) - a3*ln(d_ij) + a4*FTA_ij + e_ij
        positive_mask = trade_arr > 0
        if positive_mask.sum() < 5:
            return {"score": 50.0, "creation": None, "diversion": None,
                    "note": "Insufficient positive trade flows for estimation"}

        ln_trade = np.log(trade_arr[positive_mask])
        ln_gdp_r = np.log(gdp_r_arr[positive_mask])
        ln_gdp_p = np.log(gdp_p_arr[positive_mask])
        ln_dist = np.log(dist_arr[positive_mask])
        fta_sub = fta_arr[positive_mask]

        # Build design matrix: [const, ln_gdp_r, ln_gdp_p, ln_dist, fta]
        n = positive_mask.sum()
        X = np.column_stack([
            np.ones(n),
            ln_gdp_r,
            ln_gdp_p,
            ln_dist,
            fta_sub,
        ])

        # OLS: beta = (X'X)^{-1} X'y
        try:
            beta = np.linalg.lstsq(X, ln_trade, rcond=None)[0]
        except np.linalg.LinAlgError:
            return {"score": 50.0, "creation": None, "diversion": None,
                    "note": "Gravity estimation failed (singular matrix)"}

        fta_effect = beta[4]  # coefficient on FTA dummy

        # Predicted trade with FTA = predicted - predicted_without_fta
        fitted_with = X @ beta
        X_no_fta = X.copy()
        X_no_fta[:, 4] = 0.0
        fitted_without = X_no_fta @ beta

        # Trade creation: increase in total trade for FTA members
        fta_members = fta_sub == 1
        non_members = fta_sub == 0

        if fta_members.sum() == 0 or non_members.sum() == 0:
            return {"score": 50.0, "creation": 0.0, "diversion": 0.0,
                    "fta_coefficient": float(fta_effect),
                    "note": "No FTA variation in sample"}

        creation = float(np.sum(
            np.exp(fitted_with[fta_members]) - np.exp(fitted_without[fta_members])
        ))

        # Trade diversion: decrease in trade with non-members
        # Residuals for non-members (negative residuals suggest diversion)
        residuals_non = ln_trade[non_members] - fitted_with[non_members]
        diversion = float(-np.sum(np.minimum(residuals_non, 0.0)))

        # Net effect
        net = creation - diversion
        total_trade = float(np.sum(trade_arr[positive_mask]))

        # Score: 0 = all creation (good), 100 = all diversion (bad)
        if creation + diversion > 0:
            diversion_share = diversion / (creation + diversion)
        else:
            diversion_share = 0.5
        score = float(np.clip(diversion_share * 100, 0, 100))

        return {
            "score": score,
            "fta_coefficient": float(fta_effect),
            "creation": creation,
            "diversion": diversion,
            "net_effect": net,
            "diversion_share": float(diversion_share),
            "total_bilateral_trade": total_trade,
            "n_observations": int(n),
            "n_fta_pairs": int(fta_members.sum()),
            "gravity_coefficients": {
                "constant": float(beta[0]),
                "ln_gdp_reporter": float(beta[1]),
                "ln_gdp_partner": float(beta[2]),
                "ln_distance": float(beta[3]),
                "fta": float(beta[4]),
            },
            "reporter": reporter,
            "year": year,
        }
