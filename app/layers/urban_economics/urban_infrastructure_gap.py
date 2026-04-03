"""Urban Infrastructure Gap module.

Measures the gap in basic infrastructure services (electricity and sanitation)
in urbanizing economies. Low access rates during rapid urbanization indicate
an acute infrastructure deficit.

Sources: WDI EG.ELC.ACCS.ZS (access to electricity, % of population),
         WDI SH.STA.BASS.ZS (people using at least basic sanitation services, %).
Score = (100 - elec_access) * 0.5 + (100 - sanit_access) * 0.5.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UrbanInfrastructureGap(LayerBase):
    layer_id = "lUE"
    name = "Urban Infrastructure Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        elec_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EG.ELC.ACCS.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        sanit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SH.STA.BASS.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not elec_rows and not sanit_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        elec_access = float(elec_rows[0]["value"]) if elec_rows else None
        sanit_access = float(sanit_rows[0]["value"]) if sanit_rows else None

        if elec_access is None and sanit_access is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no infrastructure data"}

        # Compute gap components; use 50% weight each; if one is missing use the other fully
        if elec_access is not None and sanit_access is not None:
            score = float(np.clip((100.0 - elec_access) * 0.5 + (100.0 - sanit_access) * 0.5, 0, 100))
        elif elec_access is not None:
            score = float(np.clip(100.0 - elec_access, 0, 100))
        else:
            score = float(np.clip(100.0 - sanit_access, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "electricity_access_pct": round(elec_access, 2) if elec_access is not None else None,
            "sanitation_access_pct": round(sanit_access, 2) if sanit_access is not None else None,
            "electricity_gap_ppt": round(100.0 - elec_access, 2) if elec_access is not None else None,
            "sanitation_gap_ppt": round(100.0 - sanit_access, 2) if sanit_access is not None else None,
            "interpretation": (
                "Severe infrastructure deficit: large share of population lacks basic services"
                if score > 50
                else "Significant infrastructure gap" if score > 25
                else "Moderate access shortfall" if score > 10
                else "Near-universal infrastructure access"
            ),
            "_sources": ["WDI:EG.ELC.ACCS.ZS", "WDI:SH.STA.BASS.ZS"],
        }
