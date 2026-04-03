"""GDP Decomposition module.

Methodology
-----------
Decomposes GDP by three accounting identities:

1. **Expenditure side**: Y = C + I + G + (X - M)
   - Consumption (C): personal consumption expenditures
   - Investment (I): gross private domestic investment
   - Government (G): government consumption and gross investment
   - Net exports (NX): exports minus imports

2. **Income side**: Y = W + OS + T_ind + D
   - Compensation of employees (W)
   - Gross operating surplus (OS)
   - Taxes on production/imports less subsidies (T_ind)
   - Depreciation / consumption of fixed capital (D)

3. **Production (value-added) side**: Y = sum of sectoral GVA
   - Agriculture, Industry, Manufacturing, Services shares

Growth accounting follows Solow (1957):
    g_Y = alpha * g_K + (1 - alpha) * g_L + g_A
where g_A (TFP growth) is the Solow residual.

Score (0-100) reflects macroeconomic stress based on GDP growth volatility
and composition imbalances (e.g., large negative NX, collapsing investment
share). Higher score = more stress.

Sources: FRED (BEA NIPA tables), WDI
"""

import numpy as np

from app.layers.base import LayerBase


class GDPDecomposition(LayerBase):
    layer_id = "l2"
    name = "GDP Decomposition"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")

        # Fetch GDP components from the data_series table
        components = {}
        component_codes = {
            "consumption": f"GDP_C_{country}",
            "investment": f"GDP_I_{country}",
            "government": f"GDP_G_{country}",
            "exports": f"GDP_X_{country}",
            "imports": f"GDP_M_{country}",
            "gdp": f"GDP_{country}",
        }

        for label, code in component_codes.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                components[label] = {
                    "dates": [r[0] for r in rows],
                    "values": np.array([float(r[1]) for r in rows]),
                }

        if not components.get("gdp") or len(components["gdp"]["values"]) < 4:
            return {"score": 50, "results": {"error": "insufficient GDP data"}}

        gdp = components["gdp"]["values"]
        dates = components["gdp"]["dates"]

        # --- Expenditure shares ---
        expenditure_shares = {}
        for label in ("consumption", "investment", "government"):
            if label in components and len(components[label]["values"]) == len(gdp):
                share = components[label]["values"] / gdp
                expenditure_shares[label] = {
                    "mean": float(np.mean(share)),
                    "latest": float(share[-1]),
                }

        if "exports" in components and "imports" in components:
            nx = components["exports"]["values"] - components["imports"]["values"]
            if len(nx) == len(gdp):
                nx_share = nx / gdp
                expenditure_shares["net_exports"] = {
                    "mean": float(np.mean(nx_share)),
                    "latest": float(nx_share[-1]),
                }

        # --- GDP growth ---
        gdp_growth = np.diff(np.log(gdp)) * 100  # percent log-change
        growth_stats = {
            "mean": float(np.mean(gdp_growth)),
            "std": float(np.std(gdp_growth, ddof=1)) if len(gdp_growth) > 1 else 0.0,
            "latest": float(gdp_growth[-1]) if len(gdp_growth) > 0 else 0.0,
            "dates": dates[1:],
            "values": gdp_growth.tolist(),
        }

        # --- Growth accounting (Solow residual) ---
        growth_accounting = {}
        capital_code = f"CAPITAL_{country}"
        labor_code = f"LABOR_{country}"
        k_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (capital_code,),
        )
        l_rows = await db.execute_fetchall(
            "SELECT date, value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
            (labor_code,),
        )

        if k_rows and l_rows:
            k_vals = np.array([float(r[1]) for r in k_rows])
            l_vals = np.array([float(r[1]) for r in l_rows])
            min_len = min(len(gdp), len(k_vals), len(l_vals))
            if min_len >= 4:
                alpha = kwargs.get("capital_share", 0.33)
                g_y = np.diff(np.log(gdp[:min_len]))
                g_k = np.diff(np.log(k_vals[:min_len]))
                g_l = np.diff(np.log(l_vals[:min_len]))
                tfp_growth = g_y - alpha * g_k - (1 - alpha) * g_l
                growth_accounting = {
                    "capital_share": alpha,
                    "mean_gdp_growth": float(np.mean(g_y) * 100),
                    "mean_capital_contribution": float(np.mean(alpha * g_k) * 100),
                    "mean_labor_contribution": float(np.mean((1 - alpha) * g_l) * 100),
                    "mean_tfp_growth": float(np.mean(tfp_growth) * 100),
                    "tfp_series": (tfp_growth * 100).tolist(),
                }

        # --- Contribution to growth ---
        contributions = {}
        for label in ("consumption", "investment", "government"):
            if label in components and len(components[label]["values"]) == len(gdp):
                comp_diff = np.diff(components[label]["values"])
                gdp_lag = gdp[:-1]
                contrib = (comp_diff / gdp_lag) * 100
                contributions[label] = {
                    "mean": float(np.mean(contrib)),
                    "latest": float(contrib[-1]) if len(contrib) > 0 else 0.0,
                }

        if "exports" in components and "imports" in components:
            nx = components["exports"]["values"] - components["imports"]["values"]
            if len(nx) == len(gdp):
                nx_diff = np.diff(nx)
                contrib = (nx_diff / gdp[:-1]) * 100
                contributions["net_exports"] = {
                    "mean": float(np.mean(contrib)),
                    "latest": float(contrib[-1]) if len(contrib) > 0 else 0.0,
                }

        # --- Score: stress from volatility + composition imbalance ---
        vol_score = min(float(growth_stats["std"]) * 10, 50)  # high vol -> stress

        imbalance = 0.0
        inv_share = expenditure_shares.get("investment", {}).get("latest", 0.2)
        if inv_share < 0.15:
            imbalance += 20  # collapsing investment
        nx_share = expenditure_shares.get("net_exports", {}).get("latest", 0.0)
        if abs(nx_share) > 0.05:
            imbalance += min(abs(nx_share) * 200, 30)  # large trade imbalance

        score = min(vol_score + imbalance, 100)

        return {
            "score": round(score, 1),
            "results": {
                "country": country,
                "n_obs": len(gdp),
                "period": f"{dates[0]} to {dates[-1]}",
                "expenditure_shares": expenditure_shares,
                "growth": growth_stats,
                "contributions_to_growth": contributions,
                "growth_accounting": growth_accounting,
            },
        }
