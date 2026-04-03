"""Industry diversification: structural economic diversification away from agriculture.

Economic diversification measures the extent to which an economy has
transformed its structure from agriculture-dependent to a balanced mix of
industry and services. Diversification is strongly associated with resilience
to commodity shocks, productivity growth, and long-run development (Imbs
& Wacziarg 2003; McMillan & Rodrik 2011).

Structural composition (% of GDP, World Bank WDI):
    Agriculture (NV.AGR.TOTL.ZS): primary sector, low productivity, volatile
    Manufacturing (NV.IND.MANF.ZS): secondary, medium-high productivity
    Services (NV.SRV.TOTL.ZS): tertiary, mixed productivity (formal vs informal)

Diversification index = services_pct + manufacturing_pct (modern sectors)
Monostructure stress = agriculture_pct (primary dependence)

A high agriculture share combined with low services + manufacturing indicates
structural underdevelopment and exposure to weather, commodity price, and
terms-of-trade shocks.

Score construction:
    ag_share = NV.AGR.TOTL.ZS (agriculture %)
    modern_share = NV.SRV.TOTL.ZS + NV.IND.MANF.ZS (services + manufacturing %)
    score = clip(ag_share * 2.5, 0, 100)
    High agriculture share (> 40%) -> near-CRISIS; diversified (<10% ag) -> STABLE.

References:
    Imbs, J. & Wacziarg, R. (2003). Stages of diversification. AER 93(1): 63-86.
    McMillan, M. & Rodrik, D. (2011). Globalization, structural change and
        productivity growth. NBER WP 17143.
    World Bank WDI: NV.AGR.TOTL.ZS, NV.IND.MANF.ZS, NV.SRV.TOTL.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class IndustryDiversification(LayerBase):
    layer_id = "l14"
    name = "Industry Diversification"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        ag_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'NV.AGR.TOTL.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        srv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'NV.SRV.TOTL.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        manf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'NV.IND.MANF.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not ag_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no agriculture share data",
            }

        ag_val = float(ag_rows[0]["value"])
        ag_year = ag_rows[0]["date"]

        srv_val = float(srv_rows[0]["value"]) if srv_rows else None
        manf_val = float(manf_rows[0]["value"]) if manf_rows else None

        modern_share = None
        if srv_val is not None and manf_val is not None:
            modern_share = srv_val + manf_val
        elif srv_val is not None:
            modern_share = srv_val

        # Score: high agriculture = high stress (monostructure)
        score = float(np.clip(ag_val * 2.5, 0.0, 100.0))

        diversification_tier = (
            "highly diversified" if ag_val < 5
            else "diversified" if ag_val < 10
            else "moderately diversified" if ag_val < 20
            else "moderately concentrated" if ag_val < 30
            else "agriculture-dominant" if ag_val < 50
            else "monostructure"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "agriculture_pct": round(ag_val, 2),
            "services_pct": round(srv_val, 2) if srv_val is not None else None,
            "manufacturing_pct": round(manf_val, 2) if manf_val is not None else None,
            "modern_sector_pct": round(modern_share, 2) if modern_share is not None else None,
            "latest_year": ag_year,
            "diversification_tier": diversification_tier,
        }
