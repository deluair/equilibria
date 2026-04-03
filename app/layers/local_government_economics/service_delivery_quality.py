"""Service Delivery Quality module.

Measures local service delivery quality using access to basic water services
(SH.H2O.BASW.ZS) and electricity access (EG.ELC.ACCS.ZS). Low access rates
signal inadequate local public service provision.

Score reflects service delivery failure: high score = poor delivery.
Score = clip((100 - water_pct) * 0.5 + (100 - elec_pct) * 0.5, 0, 100).

Sources: WDI SH.H2O.BASW.ZS, EG.ELC.ACCS.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ServiceDeliveryQuality(LayerBase):
    layer_id = "lLG"
    name = "Service Delivery Quality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        water_code = "SH.H2O.BASW.ZS"
        water_name = "basic drinking water"
        water_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (water_code, f"%{water_name}%"),
        )

        elec_code = "EG.ELC.ACCS.ZS"
        elec_name = "access to electricity"
        elec_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (elec_code, f"%{elec_name}%"),
        )

        if not water_rows and not elec_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no service delivery data"}

        water_pct = float(water_rows[0]["value"]) if water_rows else None
        elec_pct = float(elec_rows[0]["value"]) if elec_rows else None

        water_gap = float(np.clip(100.0 - water_pct, 0, 100)) if water_pct is not None else 50.0
        elec_gap = float(np.clip(100.0 - elec_pct, 0, 100)) if elec_pct is not None else 50.0

        score = float(np.clip(water_gap * 0.5 + elec_gap * 0.5, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "water_access_pct": round(water_pct, 2) if water_pct is not None else None,
            "electricity_access_pct": round(elec_pct, 2) if elec_pct is not None else None,
            "water_gap_ppt": round(water_gap, 2),
            "electricity_gap_ppt": round(elec_gap, 2),
            "interpretation": (
                "Critical service failure: large population without basic services"
                if score > 70
                else "Significant service delivery gaps" if score > 40
                else "Moderate access gaps" if score > 15
                else "Near-universal basic service access"
            ),
            "_sources": ["WDI:SH.H2O.BASW.ZS", "WDI:EG.ELC.ACCS.ZS"],
        }
