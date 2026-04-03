"""McCallum border effect estimation.

Methodology:
    Estimate the home bias in trade using the border effect framework
    of McCallum (1995) and subsequent refinements by Anderson and van
    Wincoop (2003). The border effect measures how much international
    borders reduce trade relative to intranational trade, controlling
    for economic size and distance.

    McCallum specification:
        ln(x_ij) = b0 + b1*ln(GDP_i) + b2*ln(GDP_j) - b3*ln(d_ij)
                   + b4*BORDER_ij + e_ij

    where BORDER_ij = 1 if i and j are in the same country (or customs
    union). The border effect is exp(b4), interpreted as the factor by
    which crossing a border reduces trade.

    Anderson-van Wincoop (AvW) correction: control for multilateral
    resistance terms to avoid omitted variable bias that inflates the
    McCallum border effect.

    Internal trade is proxied as: x_ii = GDP_i - TotalExports_i.

    Score (0-100): Higher score means stronger home bias (larger border
    effect), indicating more friction in international trade.

References:
    McCallum, J. (1995). "National Borders Matter: Canada-U.S. Regional
        Trade Patterns." American Economic Review, 85(3), 615-623.
    Anderson, J.E. and van Wincoop, E. (2003). "Gravity with Gravitas:
        A Solution to the Border Puzzle." American Economic Review,
        93(1), 170-192.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class BorderEffect(LayerBase):
    layer_id = "l1"
    name = "Border Effect"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate McCallum border effect with AvW correction.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 country code
            year : int - reference year
            region : str - regional grouping for internal trade (optional)
        """
        reporter = kwargs.get("reporter", "USA")
        year = kwargs.get("year", 2022)

        # Fetch bilateral + intranational trade data
        rows = await db.execute(
            """
            SELECT reporter_iso3, partner_iso3, trade_value,
                   gdp_reporter, gdp_partner, distance,
                   same_country, contiguity, common_language
            FROM bilateral_trade
            WHERE year = ? AND trade_value > 0
            """,
            (year,),
        )
        records = await rows.fetchall()

        if not records:
            return {"score": 50.0, "border_effect": None,
                    "note": "No bilateral trade data available"}

        # Also fetch internal trade (GDP - exports) for each country
        internal_rows = await db.execute(
            """
            SELECT iso3, gdp, total_exports
            FROM country_indicators
            WHERE year = ?
            """,
            (year,),
        )
        internal_records = await internal_rows.fetchall()

        internal_trade = {}
        gdp_map = {}
        for r in internal_records:
            iso = r["iso3"]
            gdp = float(r["gdp"] or 0)
            exports = float(r["total_exports"] or 0)
            internal_trade[iso] = max(gdp - exports, 1.0)
            gdp_map[iso] = gdp

        # Build dataset: bilateral + intranational observations
        ln_trade = []
        ln_gdp_r = []
        ln_gdp_p = []
        ln_dist = []
        border_dummy = []  # 1 = same country (internal), 0 = international
        reporter_fe = []
        partner_fe = []

        countries = sorted(set(r["reporter_iso3"] for r in records))
        c_idx = {c: i for i, c in enumerate(countries)}

        # International observations
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
            border_dummy.append(0)  # international = no border benefit
            reporter_fe.append(c_idx.get(r["reporter_iso3"], 0))
            partner_fe.append(c_idx.get(r["partner_iso3"], 0))

        # Intranational observations (internal trade)
        for iso, x_ii in internal_trade.items():
            if iso not in gdp_map or gdp_map[iso] <= 0:
                continue
            if x_ii <= 0:
                continue
            # Internal distance proxy: 0.33 * sqrt(area / pi) -- simplified as 100km
            internal_dist = 100.0

            ln_trade.append(np.log(x_ii))
            g = np.log(gdp_map[iso])
            ln_gdp_r.append(g)
            ln_gdp_p.append(g)
            ln_dist.append(np.log(internal_dist))
            border_dummy.append(1)  # internal = border benefit
            reporter_fe.append(c_idx.get(iso, 0))
            partner_fe.append(c_idx.get(iso, 0))

        n = len(ln_trade)
        if n < 10:
            return {"score": 50.0, "border_effect": None,
                    "note": "Insufficient observations for estimation"}

        y = np.array(ln_trade)
        X_basic = np.column_stack([
            np.ones(n),
            np.array(ln_gdp_r),
            np.array(ln_gdp_p),
            np.array(ln_dist),
            np.array(border_dummy, dtype=float),
        ])

        # McCallum OLS
        try:
            beta_mc = np.linalg.lstsq(X_basic, y, rcond=None)[0]
        except np.linalg.LinAlgError:
            return {"score": 50.0, "border_effect": None,
                    "note": "McCallum estimation failed"}

        border_coeff_mc = beta_mc[4]
        border_effect_mc = float(np.exp(border_coeff_mc))

        # Anderson-van Wincoop: add exporter and importer fixed effects
        # to control for multilateral resistance
        n_c = len(countries)
        if n_c > 2:
            # Create FE dummies (drop first category)
            reporter_dummies = np.zeros((n, n_c - 1))
            partner_dummies = np.zeros((n, n_c - 1))
            for i in range(n):
                if reporter_fe[i] > 0:
                    reporter_dummies[i, reporter_fe[i] - 1] = 1.0
                if partner_fe[i] > 0:
                    partner_dummies[i, partner_fe[i] - 1] = 1.0

            X_avw = np.column_stack([
                X_basic,
                reporter_dummies,
                partner_dummies,
            ])

            try:
                beta_avw = np.linalg.lstsq(X_avw, y, rcond=None)[0]
                border_coeff_avw = beta_avw[4]
                border_effect_avw = float(np.exp(border_coeff_avw))
            except np.linalg.LinAlgError:
                border_coeff_avw = border_coeff_mc
                border_effect_avw = border_effect_mc
        else:
            border_coeff_avw = border_coeff_mc
            border_effect_avw = border_effect_mc

        # Residual analysis for reporter
        reporter_obs = [i for i in range(n)
                        if reporter_fe[i] == c_idx.get(reporter, -1)]
        if reporter_obs:
            fitted = X_basic[reporter_obs] @ beta_mc
            residuals = y[reporter_obs] - fitted
            avg_residual = float(np.mean(residuals))
        else:
            avg_residual = 0.0

        # R-squared
        ss_res = float(np.sum((y - X_basic @ beta_mc) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Score: larger border effect = more friction = higher score
        # McCallum found ~22x for Canada-US. AvW corrects to ~5-10x.
        # Normalize: border_effect of 1 (no friction) = 0, 20+ = 100
        score = float(np.clip((border_effect_avw - 1) / 19 * 100, 0, 100))

        return {
            "score": score,
            "border_effect_mccallum": border_effect_mc,
            "border_coeff_mccallum": float(border_coeff_mc),
            "border_effect_avw": border_effect_avw,
            "border_coeff_avw": float(border_coeff_avw),
            "gravity_coefficients": {
                "constant": float(beta_mc[0]),
                "ln_gdp_reporter": float(beta_mc[1]),
                "ln_gdp_partner": float(beta_mc[2]),
                "ln_distance": float(beta_mc[3]),
                "border": float(beta_mc[4]),
            },
            "r_squared": float(r_squared),
            "n_observations": n,
            "n_countries": n_c,
            "n_internal_obs": int(np.sum(np.array(border_dummy))),
            "reporter_avg_residual": avg_residual,
            "reporter": reporter,
            "year": year,
        }
