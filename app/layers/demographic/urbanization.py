"""Urbanization: urban population share and growth rate stress.

Urbanization creates economic opportunities through agglomeration but imposes
infrastructure, housing, and service delivery stress when it occurs too rapidly
or reaches extreme levels without commensurate institutional capacity.

Very high urban share (>80%) concentrates risk in dense areas and strains
municipal services. Rapid urban growth (>4%/yr) overwhelms planning cycles,
producing slums, congestion, and environmental degradation (Jedwab et al. 2017).
The stress is non-linear: moderate urbanization (40-70%) with controlled growth
is associated with productivity gains; extremes in either direction raise costs.

Urban share score: penalty rises above 60% and sharply above 80%.
Urban growth score: penalty rises above 2%/yr and sharply above 4%/yr.
Composite = 0.6 * share_score + 0.4 * growth_score.

References:
    Jedwab, R., Christiaensen, L. & Gindelsky, M. (2017). Demography, urbanization
        and development. J. of Urban Economics, 98, 92-107.
    Glaeser, E. (2011). Triumph of the City. Macmillan.
    UN-Habitat (2022). World Cities Report 2022. Nairobi.

Score: 0 = no stress, 100 = extreme urbanization stress.
Series: SP.URB.TOTL.IN.ZS (urban % of total), SP.URB.GROW (urban growth rate %).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class Urbanization(LayerBase):
    layer_id = "l17"
    name = "Urbanization"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        if not country_iso3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "country_iso3 required",
            }

        urb_share_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.URB.TOTL.IN.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date
            """,
            (country_iso3,),
        )

        urb_growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.URB.GROW'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date
            """,
            (country_iso3,),
        )

        if not urb_share_rows and not urb_growth_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"no urbanization data for {country_iso3}",
            }

        # Latest urban share
        urban_share = None
        share_year = None
        if urb_share_rows:
            latest = urb_share_rows[-1]
            urban_share = float(latest["value"])
            share_year = latest["date"][:4]

        # Latest and average urban growth rate
        urban_growth = None
        growth_year = None
        avg_growth_5yr = None
        if urb_growth_rows:
            latest_g = urb_growth_rows[-1]
            urban_growth = float(latest_g["value"])
            growth_year = latest_g["date"][:4]
            recent = urb_growth_rows[-5:]
            vals = [float(r["value"]) for r in recent if r["value"] is not None]
            if vals:
                avg_growth_5yr = round(float(np.mean(vals)), 3)

        # --- Share score (0-100) ---
        # Stress rises above 60%, sharply above 80%
        share_score = 0.0
        if urban_share is not None:
            if urban_share <= 40:
                share_score = urban_share * 0.3          # 0-12
            elif urban_share <= 60:
                share_score = 12 + (urban_share - 40) * 0.9  # 12-30
            elif urban_share <= 80:
                share_score = 30 + (urban_share - 60) * 1.75  # 30-65
            else:
                share_score = 65 + (urban_share - 80) * 1.75  # 65+, capped below

        share_score = float(np.clip(share_score, 0, 100))

        # --- Growth score (0-100) ---
        # Stress rises above 2%/yr, sharply above 4%/yr
        growth_score = 0.0
        rate = avg_growth_5yr if avg_growth_5yr is not None else urban_growth
        if rate is not None:
            if rate <= 0:
                growth_score = 0.0
            elif rate <= 2:
                growth_score = rate * 10             # 0-20
            elif rate <= 4:
                growth_score = 20 + (rate - 2) * 20  # 20-60
            else:
                growth_score = 60 + (rate - 4) * 10  # 60+

        growth_score = float(np.clip(growth_score, 0, 100))

        # Composite
        if urban_share is not None and rate is not None:
            score = 0.6 * share_score + 0.4 * growth_score
        elif urban_share is not None:
            score = share_score
        else:
            score = growth_score

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country_iso3,
                "urban_share_pct": round(urban_share, 2) if urban_share is not None else None,
                "share_year": share_year,
                "urban_growth_rate_pct": round(urban_growth, 3) if urban_growth is not None else None,
                "avg_growth_5yr_pct": avg_growth_5yr,
                "growth_year": growth_year,
                "share_score": round(share_score, 2),
                "growth_score": round(growth_score, 2),
                "stress_type": (
                    "hyper-urban" if urban_share is not None and urban_share > 80
                    else "rapid-growth" if rate is not None and rate > 4
                    else "moderate"
                ),
            },
        }
