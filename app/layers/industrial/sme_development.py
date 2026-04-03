"""SME development: private credit access and regulatory cost barriers.

Small and medium enterprises (SMEs) account for over 90% of firms and more
than 50% of employment worldwide (IFC 2017). They are the primary engine of
job creation, innovation diffusion, and economic dynamism in developing economies.
SME development depends critically on two factors:

1. Credit access (FS.AST.PRVT.GD.ZS): domestic credit to the private sector
   as % of GDP proxies financial system depth. Low private credit constrains
   SME working capital, investment, and scaling. Beck & Demirguc-Kunt (2006)
   find that financial development is more growth-promoting for smaller firms.

2. Regulatory barriers (IC.REG.COST.PC.ZS): business registration cost as %
   of per-capita GNI proxies the burden of formal sector entry. High costs
   push SMEs into informality, limiting their access to finance, legal
   recourse, and public procurement.

Dual-stress signal: low private credit AND high registration cost = severe SME
development deficit. The combination reflects both the supply side (banks won't
lend to small firms) and the demand side (formalization is too costly).

Score construction:
    credit_stress = max(0, 40 - private_credit_pct) * 1.5  [<40% GDP = stress]
    cost_stress   = clip(reg_cost_pct * 5, 0, 50)          [high cost = stress]
    Weights: credit 0.6, cost 0.4 (credit access more binding)
    If only one available: use that indicator scaled to 0-100.

References:
    Beck, T. & Demirguc-Kunt, A. (2006). Small and medium-size enterprises:
        Access to finance as a growth constraint. JBF 30(11): 2931-2943.
    IFC (2017). MSME Finance Gap. Washington DC: IFC.
    World Bank WDI: FS.AST.PRVT.GD.ZS, IC.REG.COST.PC.ZS.

Indicators:
    FS.AST.PRVT.GD.ZS (Domestic credit to private sector, % of GDP)
    IC.REG.COST.PC.ZS (Cost of business start-up procedures, % of GNI per capita)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SMEDevelopment(LayerBase):
    layer_id = "l14"
    name = "SME Development"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        credit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'FS.AST.PRVT.GD.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        cost_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'IC.REG.COST.PC.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        has_credit = bool(credit_rows)
        has_cost = bool(cost_rows)

        if not has_credit and not has_cost:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no SME development proxy data",
            }

        credit_pct = float(credit_rows[0]["value"]) if has_credit else None
        credit_year = credit_rows[0]["date"] if has_credit else None
        cost_pct = float(cost_rows[0]["value"]) if has_cost else None
        cost_year = cost_rows[0]["date"] if has_cost else None

        credit_stress = None
        cost_stress = None

        if credit_pct is not None:
            credit_stress = float(np.clip(max(0.0, 40.0 - credit_pct) * 1.5, 0.0, 60.0))

        if cost_pct is not None:
            cost_stress = float(np.clip(cost_pct * 5.0, 0.0, 50.0))

        if credit_stress is not None and cost_stress is not None:
            # Scale cost_stress to 0-100, then weight
            raw = 0.6 * (credit_stress / 60.0 * 100.0) + 0.4 * (cost_stress / 50.0 * 100.0)
            score = float(np.clip(raw, 0.0, 100.0))
            method = "composite"
        elif credit_stress is not None:
            score = float(np.clip(credit_stress / 60.0 * 100.0, 0.0, 100.0))
            method = "credit_only"
        else:
            score = float(np.clip(cost_stress / 50.0 * 100.0, 0.0, 100.0))
            method = "cost_only"

        sme_tier = (
            "favorable" if score < 25
            else "moderate" if score < 50
            else "constrained" if score < 75
            else "severely constrained"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "private_credit_pct_gdp": round(credit_pct, 2) if credit_pct is not None else None,
            "credit_year": credit_year,
            "registration_cost_pct_gni": round(cost_pct, 2) if cost_pct is not None else None,
            "cost_year": cost_year,
            "credit_stress_component": round(credit_stress, 2) if credit_stress is not None else None,
            "cost_stress_component": round(cost_stress, 2) if cost_stress is not None else None,
            "scoring_method": method,
            "sme_environment_tier": sme_tier,
        }
