"""Remittance flow determinants and multiplier effects.

Models remittance inflows as a function of migrant stock abroad, income
differentials between host and home countries, and transaction costs.
Estimates the fiscal/GDP multiplier of remittances on the receiving economy.

Specification:
    ln(R_ij) = b0 + b1*ln(MigStock_ij) + b2*ln(Y_j/Y_i) + b3*Cost_ij
               + b4*ln(FinDev_i) + b5*XRate_volatility + e_ij

where R_ij is remittance flow from country j to home country i.

Multiplier estimation:
    dY/dR = 1 / (1 - c*(1-t)*(1-m))

where c = MPC out of remittances, t = tax rate, m = import leakage.
Empirical estimates: 1.5-3.0 for developing countries (Adams & Page 2005).

Key stylized facts (World Bank):
    - Bangladesh: 2nd largest South Asian remittance recipient
    - Transaction costs negatively affect flows (SDG target: <3%)
    - Remittances are countercyclical to home-country income shocks
    - Financial development increases formal channel usage

References:
    Adams, R. & Page, J. (2005). Do International Migration and Remittances
        Reduce Poverty in Developing Countries? World Development 33(10).
    Rapoport, H. & Docquier, F. (2006). The Economics of Migrants'
        Remittances. Handbook of the Economics of Giving, Altruism and
        Reciprocity, Vol. 2.
    Freund, C. & Spatafora, N. (2008). Remittances, Transaction Costs,
        and Informality. Journal of Development Economics 86(2): 356-366.

Score: high transaction costs + low multiplier -> STRESS. Healthy remittance
channel (low costs, high formal share) -> STABLE.
"""

import numpy as np

from app.layers.base import LayerBase


class RemittanceDeterminants(LayerBase):
    layer_id = "l3"
    name = "Remittance Determinants"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params = [country, "remittance"]
        if year:
            params.append(str(year))

        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = ?
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient remittance data"}

        import json

        ln_remit = []
        features = []

        for row in rows:
            remit = row["value"]
            if remit is None or remit <= 0:
                continue
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            mig_stock = meta.get("migrant_stock")
            income_ratio = meta.get("income_ratio")  # host/home GDP per capita
            tx_cost = meta.get("transaction_cost_pct")
            fin_dev = meta.get("financial_development")  # domestic credit / GDP
            if mig_stock is None or income_ratio is None:
                continue
            if mig_stock <= 0 or income_ratio <= 0:
                continue

            ln_remit.append(np.log(remit))
            features.append([
                1.0,
                np.log(mig_stock),
                np.log(income_ratio),
                float(tx_cost) if tx_cost is not None else 5.0,
                np.log(max(fin_dev, 0.01)) if fin_dev is not None else 0.0,
            ])

        n = len(ln_remit)
        if n < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        y = np.array(ln_remit)
        X = np.array(features)

        # OLS estimation
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - X @ beta
        ss_res = np.sum(resid ** 2)
        ss_tot = np.sum((y - y.mean()) ** 2)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        n_k = n - X.shape[1]
        sigma2 = ss_res / n_k if n_k > 0 else ss_res
        XtX_inv = np.linalg.pinv(X.T @ X)
        se = np.sqrt(np.maximum(np.diag(sigma2 * XtX_inv), 0.0))

        # Multiplier estimation
        # Use average transaction cost from data
        avg_tx_cost = float(np.mean([f[3] for f in features]))
        mpc_remittance = 0.70  # typical MPC from remittance literature
        tax_rate = 0.15
        import_leakage = 0.30
        multiplier = 1.0 / (1.0 - mpc_remittance * (1.0 - tax_rate) * (1.0 - import_leakage))

        # Elasticities
        migrant_stock_elasticity = float(beta[1])
        income_ratio_elasticity = float(beta[2])
        tx_cost_effect = float(beta[3])

        # Score: high tx costs and low multiplier -> stress
        # SDG target is <3% transaction cost
        if avg_tx_cost > 10:
            score = 70.0
        elif avg_tx_cost > 5:
            score = 40.0 + (avg_tx_cost - 5) * 6.0
        else:
            score = 10.0 + avg_tx_cost * 6.0
        score = max(0.0, min(100.0, score))

        coef_names = ["constant", "ln_migrant_stock", "ln_income_ratio",
                      "transaction_cost", "ln_financial_dev"]

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "coefficients": dict(zip(coef_names, beta.tolist())),
            "std_errors": dict(zip(coef_names, se.tolist())),
            "r_squared": round(r2, 4),
            "elasticities": {
                "migrant_stock": round(migrant_stock_elasticity, 4),
                "income_ratio": round(income_ratio_elasticity, 4),
                "transaction_cost": round(tx_cost_effect, 4),
            },
            "multiplier": {
                "value": round(multiplier, 2),
                "mpc_assumed": mpc_remittance,
                "avg_transaction_cost_pct": round(avg_tx_cost, 2),
            },
        }
