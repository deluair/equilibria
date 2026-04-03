"""Resource Politics module.

Political resource curse: resource rents x governance gap interaction.

Theory:
    Ross (2001) and Mehlum et al. (2006) document the resource curse mechanism:
    high natural resource rents reduce democratic accountability and weaken
    governance when institutions are poor. The interaction effect (high rents
    combined with poor corruption control) predicts rent-seeking, weak tax
    diversification, and authoritarian drift. Collier & Hoeffler (2004) show
    resource rents increase conflict risk when combined with weak institutions.

Indicators:
    - NY.GDP.TOTL.RT.ZS: Total natural resource rents (% of GDP). Source: WDI.
    - CC.EST: Control of Corruption (WGI). Range -2.5 to 2.5.

Score construction:
    rents_stress = clip(rents_pct / 30, 0, 1)  [30% of GDP = maximum stress]
    governance_gap = clip(0.5 - cc_latest * 0.2, 0, 1)  [poor control = high gap]
    interaction = rents_stress * governance_gap  [resource curse intensity]
    score = clip((rents_stress * 0.3 + governance_gap * 0.3 + interaction * 0.4) * 100, 0, 100)

References:
    Ross, M. (2001). "Does Oil Hinder Democracy?" World Politics 53(3).
    Mehlum, H., Moene, K. & Torvik, R. (2006). "Institutions and the Resource Curse."
        Economic Journal 116(508).
    Collier, P. & Hoeffler, A. (2004). "Greed and Grievance in Civil War." OEP 56(4).
    World Bank. (2023). World Development Indicators.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ResourcePolitics(LayerBase):
    layer_id = "l12"
    name = "Resource Politics"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate resource curse political stress.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
        """
        country = kwargs.get("country_iso3", "BGD")

        rents_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%NY.GDP.TOTL.RT.ZS%' OR ds.name LIKE '%total%natural%resource%rents%gdp%'
                   OR ds.name LIKE '%resource%rents%percent%gdp%')
            ORDER BY dp.date
            """,
            (country,),
        )

        cc_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%CC.EST%' OR ds.name LIKE '%control%corruption%estimate%'
                   OR ds.name LIKE '%control%of%corruption%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rents_rows and not cc_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no resource politics data"}

        rents_latest = 0.0
        rents_stress = 0.0
        rents_detail = None
        if rents_rows:
            rents = np.array([float(r["value"]) for r in rents_rows])
            rents_latest = float(rents[-1])
            rents_stress = float(np.clip(rents_latest / 30.0, 0, 1))
            rents_detail = {
                "latest_pct_gdp": round(rents_latest, 3),
                "mean_pct_gdp": round(float(np.mean(rents)), 3),
                "n_obs": len(rents),
                "date_range": [str(rents_rows[0]["date"]), str(rents_rows[-1]["date"])],
            }

        cc_latest = 0.0
        governance_gap = 0.5
        cc_detail = None
        if cc_rows:
            cc = np.array([float(r["value"]) for r in cc_rows])
            cc_latest = float(cc[-1])
            governance_gap = float(np.clip(0.5 - cc_latest * 0.2, 0, 1))
            cc_detail = {
                "latest": round(cc_latest, 4),
                "mean": round(float(np.mean(cc)), 4),
                "n_obs": len(cc),
                "date_range": [str(cc_rows[0]["date"]), str(cc_rows[-1]["date"])],
            }

        interaction = rents_stress * governance_gap

        score = float(np.clip(
            (rents_stress * 0.3 + governance_gap * 0.3 + interaction * 0.4) * 100,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "rents_stress": round(rents_stress * 0.3 * 100, 2),
                "governance_gap_stress": round(governance_gap * 0.3 * 100, 2),
                "interaction_stress": round(interaction * 0.4 * 100, 2),
            },
            "resource_curse_intensity": round(interaction, 4),
            "resource_curse_risk": (
                "high" if score > 60 else "moderate" if score > 30 else "low"
            ),
            "reference": "Ross 2001; Mehlum et al. 2006; Collier & Hoeffler 2004; WDI + WGI",
        }

        if rents_detail:
            result["resource_rents"] = rents_detail
        if cc_detail:
            result["corruption_control"] = cc_detail

        return result
