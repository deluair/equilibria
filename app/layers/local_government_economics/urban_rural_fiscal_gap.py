"""Urban-Rural Fiscal Gap module.

Proxies the urban-rural fiscal gap using urban population share (SP.URB.TOTL.IN.ZS)
and electricity access (EG.ELC.ACCS.ZS). Low electricity access in countries
with high urbanization implies a large rural exclusion gap; the fiscal gap is
largest where urbanization concentrates resources in cities while rural areas
remain underserved.

Gap proxy = (urb_pct / 100) * (100 - elec_pct): large when cities dominate
and rural electricity access is low.
Score = clip(gap_proxy / 50 * 100, 0, 100), anchoring 50 index points at full stress.

Sources: WDI SP.URB.TOTL.IN.ZS, EG.ELC.ACCS.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_MAX_GAP_PROXY = 50.0  # gap proxy value considered maximum stress


class UrbanRuralFiscalGap(LayerBase):
    layer_id = "lLG"
    name = "Urban-Rural Fiscal Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urb_code = "SP.URB.TOTL.IN.ZS"
        urb_name = "urban population"
        urb_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (urb_code, f"%{urb_name}%"),
        )

        elec_code = "EG.ELC.ACCS.ZS"
        elec_name = "access to electricity"
        elec_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (elec_code, f"%{elec_name}%"),
        )

        if not urb_rows and not elec_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no urban-rural gap data"}

        urb_pct = float(urb_rows[0]["value"]) if urb_rows else None
        elec_pct = float(elec_rows[0]["value"]) if elec_rows else None

        if urb_pct is None or elec_pct is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient urban-rural data"}

        gap_proxy = (urb_pct / 100.0) * (100.0 - elec_pct)
        score = float(np.clip(gap_proxy / _MAX_GAP_PROXY * 100.0, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urban_population_pct": round(urb_pct, 2),
            "electricity_access_pct": round(elec_pct, 2),
            "gap_proxy": round(gap_proxy, 2),
            "interpretation": (
                "Large urban-rural fiscal gap: rural exclusion is severe"
                if score > 65
                else "Significant urban concentration with rural underservice" if score > 40
                else "Moderate urban-rural fiscal disparity" if score > 20
                else "Low urban-rural fiscal gap"
            ),
            "_sources": ["WDI:SP.URB.TOTL.IN.ZS", "WDI:EG.ELC.ACCS.ZS"],
        }
