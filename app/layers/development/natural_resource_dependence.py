"""Natural resource dependence: resource curse test via rents and growth.

Examines whether high natural resource rents combined with low growth
signal a resource curse. Distinct from ResourceCurse (which runs full
cross-country OLS with institutional interaction); this module computes
a direct country-level score based on rents/GDP and GDP growth.

Key references:
    Sachs, J. & Warner, A. (1995). Natural resource abundance and economic
        growth. NBER Working Paper 5398.
    van der Ploeg, F. (2011). Natural resources: curse or blessing? Journal
        of Economic Literature, 49(2), 366-420.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

GROWTH_THRESHOLD = 3.0  # % annual GDP growth considered adequate


class NaturalResourceDependence(LayerBase):
    layer_id = "l4"
    name = "Natural Resource Dependence"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Resource curse score: rents * (1 + growth_penalty).

        Queries NY.GDP.TOTL.RT.ZS (total natural resource rents % GDP) and
        NY.GDP.MKTP.KD.ZG (GDP growth %). Score = clip(rents * (1 + max(0, 3-g)/3), 0, 100).
        High rents + low growth = resource curse stress.

        Returns dict with score, rents %, growth %, curse indicator.
        """
        country_iso3 = kwargs.get("country_iso3")

        rents_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.TOTL.RT.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        growth_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not rents_rows or not growth_rows:
            return {"score": 50, "results": {"error": "insufficient resource rents or growth data"}}

        rents_data: dict[str, dict[str, float]] = {}
        for r in rents_rows:
            rents_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        growth_data: dict[str, dict[str, float]] = {}
        for r in growth_rows:
            growth_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Global high-rents countries (context)
        high_rents = []
        for iso in set(rents_data.keys()) & set(growth_data.keys()):
            r_years = sorted(rents_data[iso].keys())
            g_years = sorted(growth_data[iso].keys())
            if not r_years or not g_years:
                continue
            avg_rents = np.mean(list(rents_data[iso].values()))
            avg_growth = np.mean(list(growth_data[iso].values()))
            if avg_rents > 10:
                high_rents.append({"iso": iso, "avg_rents": avg_rents, "avg_growth": avg_growth})

        high_rents.sort(key=lambda x: x["avg_rents"], reverse=True)

        # Target country
        target_analysis = None
        score = 30.0  # default: not resource dependent

        if country_iso3 and country_iso3 in rents_data:
            r_years = sorted(rents_data[country_iso3].keys())
            g_years = sorted(growth_data.get(country_iso3, {}).keys())

            latest_rents = rents_data[country_iso3][r_years[-1]] if r_years else None
            avg_rents = float(np.mean(list(rents_data[country_iso3].values()))) if r_years else None

            recent_growth = None
            if g_years and country_iso3 in growth_data:
                recent_growth = float(np.mean([
                    growth_data[country_iso3][yr]
                    for yr in g_years[-5:]
                    if yr in growth_data[country_iso3]
                ]))

            if latest_rents is not None:
                growth_penalty = max(0.0, GROWTH_THRESHOLD - (recent_growth or GROWTH_THRESHOLD)) / GROWTH_THRESHOLD
                raw_score = latest_rents * (1 + growth_penalty)
                score = float(np.clip(raw_score, 0, 100))

                target_analysis = {
                    "latest_rents_pct_gdp": latest_rents,
                    "avg_rents_pct_gdp": avg_rents,
                    "recent_5yr_growth": recent_growth,
                    "growth_threshold": GROWTH_THRESHOLD,
                    "resource_dependent": latest_rents > 10,
                    "highly_dependent": latest_rents > 20,
                    "low_growth": recent_growth is not None and recent_growth < GROWTH_THRESHOLD,
                    "curse_signal": latest_rents > 10 and (
                        recent_growth is None or recent_growth < GROWTH_THRESHOLD
                    ),
                }

        return {
            "score": score,
            "results": {
                "high_rents_countries": high_rents[:10],
                "growth_threshold_pct": GROWTH_THRESHOLD,
                "target": target_analysis,
                "country_iso3": country_iso3,
            },
        }
