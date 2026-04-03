"""Reshoring capacity: manufacturing base strength composite.

Uses NV.IND.MANF.ZS (manufacturing value added as % of GDP) and
NE.GDI.TOTL.ZS (gross capital formation as % of GDP). Together they proxy a
country's capacity to reshore or nearshore production.

Methodology:
    Fetch latest available value for each indicator.
    Normalize:
        manf_norm = clip(manf_share * 2, 0, 100)  -- 50% manf/GDP is ceiling
        gdi_norm  = clip(gdi_share * 2.5, 0, 100) -- 40% investment/GDP is ceiling
    Composite = (manf_norm + gdi_norm) / 2 (average of available).
    Score = clip(100 - composite, 0, 100) -- inverted: higher capacity = lower stress.

    composite = 100: score = 0 (strong reshoring capacity).
    composite = 0:   score = 100 (no reshoring capacity).

Score (0-100): Higher score indicates weaker reshoring capacity.

References:
    World Bank WDI NV.IND.MANF.ZS and NE.GDI.TOTL.ZS.
    Fratocchi et al. (2014). "When manufacturing moves back: Concepts and questions." JOM.
    Bailey & De Propris (2014). "Manufacturing reshoring and its limits." Cambridge J. Regions.
"""

from __future__ import annotations

from app.layers.base import LayerBase

_MANF_CODE = "NV.IND.MANF.ZS"
_GDI_CODE = "NE.GDI.TOTL.ZS"


class ReshoringCapacity(LayerBase):
    layer_id = "lSR"
    name = "Reshoring Capacity"

    async def _fetch_latest(self, db, code: str, name: str) -> float | None:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        for r in rows:
            if r["value"] is not None:
                return float(r["value"])
        return None

    async def compute(self, db, **kwargs) -> dict:
        manf = await self._fetch_latest(db, _MANF_CODE, "manufacturing value added")
        gdi = await self._fetch_latest(db, _GDI_CODE, "gross capital formation")

        if manf is None and gdi is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for NV.IND.MANF.ZS or NE.GDI.TOTL.ZS",
            }

        components: list[float] = []
        if manf is not None:
            manf_norm = min(max(manf * 2.0, 0.0), 100.0)
            components.append(manf_norm)
        if gdi is not None:
            gdi_norm = min(max(gdi * 2.5, 0.0), 100.0)
            components.append(gdi_norm)

        composite = sum(components) / len(components)
        score = float(min(max(100.0 - composite, 0.0), 100.0))

        capacity_tier = (
            "high" if composite >= 70
            else "moderate" if composite >= 40
            else "low" if composite >= 20
            else "very_low"
        )

        return {
            "score": round(score, 2),
            "manufacturing_value_added_pct_gdp": round(manf, 2) if manf is not None else None,
            "gross_capital_formation_pct_gdp": round(gdi, 2) if gdi is not None else None,
            "reshoring_capacity_composite": round(composite, 2),
            "capacity_tier": capacity_tier,
            "manf_indicator": _MANF_CODE,
            "gdi_indicator": _GDI_CODE,
        }
