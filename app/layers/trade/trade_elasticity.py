"""Trade elasticity estimation: import demand, export supply, and Armington elasticities.

Import demand elasticity measures how responsive a country's imports are to changes
in relative prices.  The standard specification (Kee, Nicita & Olarreaga 2008)
estimates:
    ln(M_k) = a + e_k * ln(PM_k / PD_k) + b * ln(Y) + u

where M_k is import quantity of product k, PM/PD is import/domestic price ratio,
Y is income, and e_k is the import demand elasticity (expected negative).

Export supply elasticity measures how responsive a country's exports are to
price changes, important for understanding terms-of-trade effects and the
Marshall-Lerner condition.

Armington elasticity (sigma) governs substitution between domestic and imported
varieties in CES demand systems.  Critical for computable general equilibrium
(CGE) trade models and welfare calculations.

The score reflects trade vulnerability: inelastic import demand (|e| < 1) combined
with inelastic export supply signals price-taking vulnerability (high score).
"""

import numpy as np
from app.layers.base import LayerBase


class TradeElasticity(LayerBase):
    layer_id = "l1"
    name = "Trade Elasticity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Fetch import value and price index data
        import_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.unit
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'comtrade', 'imf')
              AND (ds.name LIKE '%import%volume%'
                   OR ds.name LIKE '%import%price%'
                   OR ds.name LIKE '%export%volume%'
                   OR ds.name LIKE '%export%price%'
                   OR ds.name LIKE '%gdp%constant%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not import_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no trade volume/price data"}

        # Organize by series name and date
        series: dict[str, dict[str, float]] = {}
        for row in import_rows:
            name = row["name"].lower()
            date = row["date"]
            val = row["value"]
            if val is None:
                continue
            series.setdefault(name, {})[date] = val

        # Identify import volume, import price, export volume, export price, GDP
        import_vol = self._find_series(series, ["import", "volume"])
        import_price = self._find_series(series, ["import", "price"])
        export_vol = self._find_series(series, ["export", "volume"])
        export_price = self._find_series(series, ["export", "price"])
        gdp = self._find_series(series, ["gdp", "constant"])

        results = {}

        # Import demand elasticity
        import_elast = self._estimate_elasticity(import_vol, import_price, gdp, "import_demand")
        if import_elast:
            results["import_demand"] = import_elast

        # Export supply elasticity
        export_elast = self._estimate_elasticity(export_vol, export_price, gdp, "export_supply")
        if export_elast:
            results["export_supply"] = export_elast

        # Armington elasticity (substitution between domestic and foreign goods)
        armington = self._estimate_armington(import_vol, import_price, gdp)
        if armington:
            results["armington"] = armington

        if not results:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for estimation"}

        # Marshall-Lerner condition check
        ml_sum = None
        if "import_demand" in results and "export_supply" in results:
            e_m = abs(results["import_demand"]["elasticity"])
            e_x = abs(results["export_supply"]["elasticity"])
            ml_sum = e_m + e_x
            results["marshall_lerner"] = {
                "sum_elasticities": round(ml_sum, 4),
                "condition_met": ml_sum > 1.0,
                "interpretation": (
                    "Devaluation improves trade balance"
                    if ml_sum > 1.0
                    else "Devaluation may worsen trade balance (J-curve risk)"
                ),
            }

        # Score: vulnerability assessment
        # Inelastic imports (|e| < 0.5) and inelastic exports = high vulnerability
        score = self._compute_vulnerability_score(results, ml_sum)

        return {
            "score": round(score, 2),
            "country": country,
            **results,
        }

    @staticmethod
    def _find_series(
        series: dict[str, dict[str, float]], keywords: list[str]
    ) -> dict[str, float] | None:
        """Find the first series whose name contains all keywords."""
        for name, data in series.items():
            if all(kw in name for kw in keywords):
                return data
        return None

    @staticmethod
    def _estimate_elasticity(
        volume: dict[str, float] | None,
        price: dict[str, float] | None,
        income: dict[str, float] | None,
        label: str,
    ) -> dict | None:
        """Log-log OLS elasticity estimation."""
        if not volume or not price:
            return None

        common_dates = sorted(set(volume.keys()) & set(price.keys()))
        if income:
            common_dates = sorted(set(common_dates) & set(income.keys()))
        if len(common_dates) < 8:
            return None

        y_vals = np.array([volume[d] for d in common_dates])
        p_vals = np.array([price[d] for d in common_dates])

        # Filter positive values for log transform
        mask = (y_vals > 0) & (p_vals > 0)
        if income:
            inc_vals = np.array([income[d] for d in common_dates])
            mask &= inc_vals > 0
        else:
            inc_vals = None

        if np.sum(mask) < 8:
            return None

        ln_y = np.log(y_vals[mask])
        ln_p = np.log(p_vals[mask])

        if inc_vals is not None:
            ln_inc = np.log(inc_vals[mask])
            X = np.column_stack([np.ones(np.sum(mask)), ln_p, ln_inc])
        else:
            X = np.column_stack([np.ones(np.sum(mask)), ln_p])

        beta = np.linalg.lstsq(X, ln_y, rcond=None)[0]
        resid = ln_y - X @ beta
        ss_res = np.sum(resid ** 2)
        ss_tot = np.sum((ln_y - np.mean(ln_y)) ** 2)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
        n = len(ln_y)
        k = X.shape[1]
        se = np.sqrt(ss_res / (n - k) * np.diag(np.linalg.inv(X.T @ X)))

        result = {
            "elasticity": round(float(beta[1]), 4),
            "std_error": round(float(se[1]), 4),
            "t_stat": round(float(beta[1] / se[1]) if se[1] > 0 else 0.0, 4),
            "r_squared": round(float(r2), 4),
            "n_obs": int(n),
            "dates": [common_dates[0], common_dates[-1]],
        }
        if inc_vals is not None:
            result["income_elasticity"] = round(float(beta[2]), 4)
        return result

    @staticmethod
    def _estimate_armington(
        import_vol: dict[str, float] | None,
        import_price: dict[str, float] | None,
        gdp: dict[str, float] | None,
    ) -> dict | None:
        """Armington elasticity of substitution between domestic and imported goods.

        Uses the ratio of import volume to GDP (proxy for domestic absorption) as
        dependent variable, regressed on relative import prices.
        sigma = 1 - estimated coefficient on relative price.
        """
        if not import_vol or not import_price or not gdp:
            return None

        common_dates = sorted(set(import_vol.keys()) & set(import_price.keys()) & set(gdp.keys()))
        if len(common_dates) < 8:
            return None

        m = np.array([import_vol[d] for d in common_dates])
        p = np.array([import_price[d] for d in common_dates])
        g = np.array([gdp[d] for d in common_dates])

        mask = (m > 0) & (p > 0) & (g > 0)
        if np.sum(mask) < 8:
            return None

        # ln(M/GDP) = a + (sigma - 1) * ln(PM) + u
        ln_ratio = np.log(m[mask] / g[mask])
        ln_price = np.log(p[mask])
        X = np.column_stack([np.ones(np.sum(mask)), ln_price])

        beta = np.linalg.lstsq(X, ln_ratio, rcond=None)[0]
        sigma = 1.0 - beta[1]  # Armington sigma

        resid = ln_ratio - X @ beta
        n = len(ln_ratio)
        ss_res = np.sum(resid ** 2)
        se_beta = np.sqrt(ss_res / (n - 2) * np.diag(np.linalg.inv(X.T @ X)))

        return {
            "sigma": round(float(sigma), 4),
            "price_coefficient": round(float(beta[1]), 4),
            "std_error": round(float(se_beta[1]), 4),
            "n_obs": int(n),
            "interpretation": (
                "Elastic substitution (sigma > 1): imports and domestic goods are substitutes"
                if sigma > 1.0
                else "Inelastic substitution (sigma < 1): limited ability to substitute imports"
            ),
        }

    @staticmethod
    def _compute_vulnerability_score(results: dict, ml_sum: float | None) -> float:
        """Vulnerability score: 0 = resilient, 100 = highly vulnerable to price shocks."""
        scores = []

        if "import_demand" in results:
            e = abs(results["import_demand"]["elasticity"])
            # Inelastic import demand -> high vulnerability
            scores.append(max(0.0, min(100.0, (1.0 - e) * 50.0 + 25.0)))

        if "export_supply" in results:
            e = abs(results["export_supply"]["elasticity"])
            scores.append(max(0.0, min(100.0, (1.0 - e) * 50.0 + 25.0)))

        if ml_sum is not None:
            # Marshall-Lerner not met -> high vulnerability
            if ml_sum < 1.0:
                scores.append(75.0)
            else:
                scores.append(25.0)

        if "armington" in results:
            sigma = results["armington"]["sigma"]
            # Low Armington sigma -> cannot substitute away from imports
            scores.append(max(0.0, min(100.0, (2.0 - sigma) * 30.0 + 20.0)))

        return float(np.mean(scores)) if scores else 50.0
