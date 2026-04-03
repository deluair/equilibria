"""Pollution Haven Hypothesis testing via augmented gravity model.

Tests whether trade flows respond to environmental stringency differentials,
evaluating the PHH (Pollution Haven Hypothesis) and PHE (Pollution Haven Effect).
Decomposes trade-environment relationship into composition, scale, and technique
effects following Copeland & Taylor (2004).

Methodology:
    Augmented gravity model with environmental stringency:

        ln(X_ij^k) = alpha + b1*ln(GDP_i) + b2*ln(GDP_j) + b3*ln(dist_ij)
                      + b4*(ES_i - ES_j) + b5*dirty_k + b6*(ES_i - ES_j)*dirty_k
                      + gamma*Z_ij + e_ij^k

    where ES is environmental stringency index and dirty_k indicates
    pollution-intensive sectors.

    Composition effect: shift toward dirty industries as stringency falls.
    Scale effect: more output -> more pollution.
    Technique effect: higher income -> cleaner technology adoption.

References:
    Copeland, B. & Taylor, M.S. (2004). "Trade, growth, and the environment."
        Journal of Economic Literature, 42(1), 7-71.
    Levinson, A. & Taylor, M.S. (2008). "Unmasking the pollution haven effect."
        International Economic Review, 49(1), 223-254.
    Cole, M. & Elliott, R. (2003). "Determining the trade-environment composition
        effect." Journal of Environmental Economics and Management, 46(3), 363-383.
"""

from __future__ import annotations

import json

import numpy as np

from app.layers.base import LayerBase


class PollutionHaven(LayerBase):
    layer_id = "l9"
    name = "Pollution Haven"

    # Pollution-intensive sectors (SITC rev3 divisions)
    DIRTY_SECTORS = {
        "chemicals": [51, 52, 53, 54, 55, 56, 57, 58, 59],
        "metals": [67, 68, 69],
        "minerals": [66],
        "paper_pulp": [25, 64],
        "petroleum": [33, 34],
    }

    async def compute(self, db, **kwargs) -> dict:
        """Test Pollution Haven Hypothesis via augmented gravity.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            year : int - reference year (default latest)
        """
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        # Fetch bilateral trade with environmental stringency metadata
        year_clause = "AND dp.date LIKE ? || '%'" if year else ""
        params = [country]
        if year:
            params.append(str(year))

        rows = await db.fetch_all(
            f"""
            SELECT dp.value AS trade_value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('gravity', 'comtrade')
              AND ds.country_iso3 = ?
              AND dp.value > 0
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 20:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient bilateral trade data"}

        # Parse gravity + environmental variables
        trade_vals, ln_gdp_i, ln_gdp_j, ln_dist = [], [], [], []
        es_diff, dirty_flag, contig, comlang = [], [], [], []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            gdp_i = meta.get("gdp_origin")
            gdp_j = meta.get("gdp_dest")
            dist = meta.get("distance")
            es_i = meta.get("env_stringency_origin")
            es_j = meta.get("env_stringency_dest")

            if not all([gdp_i, gdp_j, dist]):
                continue
            if gdp_i <= 0 or gdp_j <= 0 or dist <= 0:
                continue

            trade_vals.append(float(row["trade_value"]))
            ln_gdp_i.append(np.log(gdp_i))
            ln_gdp_j.append(np.log(gdp_j))
            ln_dist.append(np.log(dist))

            # Environmental stringency differential
            if es_i is not None and es_j is not None:
                es_diff.append(float(es_i) - float(es_j))
            else:
                es_diff.append(0.0)

            # Classify sector as dirty
            sector_code = meta.get("sector_code", 0)
            is_dirty = any(
                sector_code in codes
                for codes in self.DIRTY_SECTORS.values()
            )
            dirty_flag.append(1.0 if is_dirty else 0.0)
            contig.append(float(meta.get("contiguity", 0)))
            comlang.append(float(meta.get("common_language", 0)))

        n = len(trade_vals)
        if n < 20:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient valid observations"}

        y = np.log(np.array(trade_vals))
        es = np.array(es_diff)
        dirty = np.array(dirty_flag)
        interaction = es * dirty

        # Design matrix: [const, ln_gdp_i, ln_gdp_j, ln_dist, contig, comlang,
        #                  es_diff, dirty, es_diff*dirty]
        X = np.column_stack([
            np.ones(n),
            np.array(ln_gdp_i),
            np.array(ln_gdp_j),
            np.array(ln_dist),
            np.array(contig),
            np.array(comlang),
            es,
            dirty,
            interaction,
        ])

        # OLS with HC1 robust standard errors
        beta, residuals, rank, sv = np.linalg.lstsq(X, y, rcond=None)
        fitted = X @ beta
        resid = y - fitted
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((y - y.mean()) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # HC1 standard errors
        k = X.shape[1]
        XtX_inv = np.linalg.pinv(X.T @ X)
        scale = n / max(n - k, 1)
        omega = np.diag(resid ** 2) * scale
        V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
        se = np.sqrt(np.maximum(np.diag(V), 0.0))

        coef_names = [
            "constant", "ln_gdp_origin", "ln_gdp_dest", "ln_distance",
            "contiguity", "common_language", "env_stringency_diff",
            "dirty_sector", "es_diff_x_dirty",
        ]

        coefficients = dict(zip(coef_names, beta.tolist()))
        std_errors = dict(zip(coef_names, se.tolist()))

        # PHH test: coefficient on interaction term (es_diff * dirty)
        # Negative = pollution haven effect (dirty sectors flow to lax countries)
        phh_coef = float(beta[8])
        phh_se = float(se[8])
        phh_t = phh_coef / phh_se if phh_se > 0 else 0.0
        phh_significant = abs(phh_t) > 1.96

        # Decomposition: scale, composition, technique effects
        decomposition = self._decompose_effects(
            beta_es=float(beta[6]),
            beta_dirty=float(beta[7]),
            beta_interaction=phh_coef,
            mean_es_diff=float(np.mean(es)),
            dirty_share=float(np.mean(dirty)),
        )

        # Race to bottom test: negative coefficient on env stringency
        rtb_evidence = float(beta[6]) < 0 and abs(float(beta[6]) / float(se[6])) > 1.96

        # Score: higher PHH evidence + race to bottom = higher stress
        phh_score = min(50, abs(phh_t) * 10) if phh_coef < 0 else 10
        rtb_score = 30 if rtb_evidence else 10
        dirty_exposure = float(np.mean(dirty)) * 20
        score = float(np.clip(phh_score + rtb_score + dirty_exposure, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "gravity_regression": {
                "coefficients": coefficients,
                "std_errors": std_errors,
                "r_squared": round(r2, 4),
            },
            "phh_test": {
                "interaction_coef": round(phh_coef, 6),
                "interaction_se": round(phh_se, 6),
                "t_stat": round(phh_t, 3),
                "significant_5pct": phh_significant,
                "phh_supported": phh_significant and phh_coef < 0,
            },
            "race_to_bottom": {
                "es_diff_coef": round(float(beta[6]), 6),
                "evidence": rtb_evidence,
            },
            "decomposition": decomposition,
            "dirty_sector_share": round(float(np.mean(dirty)), 4),
        }

    @staticmethod
    def _decompose_effects(
        beta_es: float,
        beta_dirty: float,
        beta_interaction: float,
        mean_es_diff: float,
        dirty_share: float,
    ) -> dict:
        """Decompose trade-environment relationship into scale, composition, technique.

        Following Copeland & Taylor (2004) and Cole & Elliott (2003):
        - Scale effect: proportional increase in all sectors
        - Composition effect: shift toward dirty sectors
        - Technique effect: cleaner production per unit
        """
        # Composition effect: how much dirty share responds to ES differential
        composition = beta_interaction * mean_es_diff
        # Scale effect: overall trade expansion from laxer regulation
        scale = beta_es * mean_es_diff * (1 - dirty_share)
        # Technique effect (residual): net cleanup from higher income
        technique = -(composition + scale) * 0.3  # approximate offset

        total = composition + scale + technique
        return {
            "scale_effect": round(scale, 6),
            "composition_effect": round(composition, 6),
            "technique_effect": round(technique, 6),
            "net_effect": round(total, 6),
            "dominant": "composition" if abs(composition) > abs(scale) and
                        abs(composition) > abs(technique) else
                        "scale" if abs(scale) > abs(technique) else "technique",
        }
