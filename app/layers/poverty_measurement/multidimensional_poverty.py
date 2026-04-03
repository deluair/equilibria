"""Multidimensional Poverty module.

Constructs a composite deprivation index from three dimensions following the
Alkire-Foster (2011) methodology: health (child mortality SH.DYN.MORT),
education (mean years schooling proxy SE.ADT.LITR.ZS), and living standards
(access to electricity EG.ELC.ACCS.ZS). Each dimension is normalized 0-100
(higher = more deprivation) and averaged to form the MPI proxy score.

Sources: WDI (SH.DYN.MORT, SE.ADT.LITR.ZS, EG.ELC.ACCS.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_INDICATORS = [
    ("SH.DYN.MORT", "child mortality", True),   # high = bad
    ("SE.ADT.LITR.ZS", "adult literacy rate", False),  # low = bad
    ("EG.ELC.ACCS.ZS", "electricity access", False),   # low = bad
]


class MultidimensionalPoverty(LayerBase):
    layer_id = "lPM"
    name = "Multidimensional Poverty"

    async def compute(self, db, **kwargs) -> dict:
        dim_scores: dict[str, float | None] = {}

        for code, name, high_is_bad in _INDICATORS:
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = ("
                "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (code, f"%{name}%"),
            )
            values = [float(r["value"]) for r in rows if r["value"] is not None] if rows else []
            if not values:
                dim_scores[code] = None
                continue
            latest = values[0]
            if high_is_bad:
                # child mortality: normalize 0-100 per 1000 live births, cap at 100
                dim_scores[code] = float(np.clip(latest, 0, 100))
            else:
                # literacy / electricity: 100 - value gives deprivation
                dim_scores[code] = float(np.clip(100 - latest, 0, 100))

        valid = [v for v in dim_scores.values() if v is not None]
        if not valid:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for MPI dimensions"}

        score = float(np.mean(valid))

        return {
            "score": round(score, 1),
            "dimensions_available": len(valid),
            "dimensions_total": len(_INDICATORS),
            "child_mortality_deprivation": dim_scores.get("SH.DYN.MORT"),
            "literacy_deprivation": dim_scores.get("SE.ADT.LITR.ZS"),
            "electricity_deprivation": dim_scores.get("EG.ELC.ACCS.ZS"),
            "methodology": "Alkire-Foster (2011) equal-weight composite",
        }
