"""Supply chain resilience: import dependency and domestic manufacturing capacity.

Supply chain resilience measures an economy's ability to absorb disruptions to
its production network. Countries with high import dependency and low domestic
manufacturing are structurally fragile: external shocks (pandemics, geopolitical
disruptions, shipping crises) propagate directly into output gaps and inflation.

The COVID-19 pandemic and the 2021-2022 supply chain crisis demonstrated that
just-in-time global supply chains carry systemic fragility (IMF 2022). Countries
with higher domestic value-added in manufacturing experienced smaller output drops
and faster recoveries.

Two-factor resilience framework:
    1. Import dependency: NE.IMP.GNFS.ZS (imports of goods and services, % of GDP)
       High imports relative to GDP = external exposure
    2. Domestic manufacturing: NV.IND.MANF.ZS (manufacturing, % of GDP)
       High domestic manufacturing = buffers external shocks

Resilience score (fragility):
    fragility = import_pct - manufacturing_pct (net external exposure)
    score = clip((fragility + 20) * 1.5, 0, 100)
    Interpretation: net fragility of 0 (imports = manufacturing) -> moderate;
    high positive fragility (imports >> manufacturing) -> high stress.

If only import data available: score = clip(import_pct * 1.2, 0, 100).

References:
    IMF (2022). World Economic Outlook: War Sets Back the Global Recovery. Ch. 4.
    Antras, P. & Chor, D. (2022). Global value chains. Handbook of International
        Economics Vol. 5, Elsevier.
    World Bank WDI: NE.IMP.GNFS.ZS, NV.IND.MANF.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SupplyChainResilience(LayerBase):
    layer_id = "l14"
    name = "Supply Chain Resilience"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        import_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'NE.IMP.GNFS.ZS'
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

        if not import_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no import dependency data",
            }

        import_pct = float(import_rows[0]["value"])
        import_year = import_rows[0]["date"]

        manf_pct = float(manf_rows[0]["value"]) if manf_rows else None
        manf_year = manf_rows[0]["date"] if manf_rows else None

        if manf_pct is not None:
            fragility = import_pct - manf_pct
            score = float(np.clip((fragility + 20.0) * 1.5, 0.0, 100.0))
            method = "fragility_composite"
        else:
            score = float(np.clip(import_pct * 1.2, 0.0, 100.0))
            fragility = None
            method = "import_only"

        resilience_tier = (
            "high resilience" if score < 25
            else "moderate resilience" if score < 50
            else "fragile" if score < 75
            else "highly fragile"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "import_pct_gdp": round(import_pct, 2),
            "import_year": import_year,
            "manufacturing_pct_gdp": round(manf_pct, 2) if manf_pct is not None else None,
            "manufacturing_year": manf_year,
            "net_fragility_index": round(float(fragility), 2) if fragility is not None else None,
            "scoring_method": method,
            "resilience_tier": resilience_tier,
        }
