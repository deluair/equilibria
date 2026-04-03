"""Tech Decoupling Exposure: high-tech exports share + R&D spending import reliance.

Measures a country's exposure to technology decoupling dynamics. Countries with
a high share of high-tech exports (TX.VAL.TECH.MF.ZS) may face market access
restrictions. Countries with low R&D intensity (GB.XPD.RSDV.GD.ZS) depend on
imported technology and are vulnerable to tech-export controls.

Methodology:
    hitech_export_share = latest TX.VAL.TECH.MF.ZS (% of manufactured exports)
    rnd_gdp = latest GB.XPD.RSDV.GD.ZS (R&D expenditure % of GDP)

    export_exposure = clip(hitech_export_share / 60.0, 0, 1)  -- 60% = max ref
    rnd_vulnerability = clip(1 - rnd_gdp / 5.0, 0, 1)         -- 5% GDP = frontier

    score = clip((export_exposure * 0.5 + rnd_vulnerability * 0.5) * 100, 0, 100)

Score (0-100): Higher = greater technology decoupling exposure.

References:
    Drezner, D. (2019). "Economic Statecraft in the Age of Trump." TWQ 42(3).
    Jorgenson et al. (2016). "The World KLEMS Initiative." IEA 59.
    World Bank WDI TX.VAL.TECH.MF.ZS, GB.XPD.RSDV.GD.ZS.
"""

from __future__ import annotations

from app.layers.base import LayerBase

_HITECH_CODE = "TX.VAL.TECH.MF.ZS"
_RND_CODE = "GB.XPD.RSDV.GD.ZS"


class TechDecouplingExposure(LayerBase):
    layer_id = "lGP"
    name = "Tech Decoupling Exposure"

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
        hitech = await self._fetch_latest(db, _HITECH_CODE, "high technology exports")
        rnd = await self._fetch_latest(db, _RND_CODE, "research development expenditure")

        if hitech is None and rnd is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for TX.VAL.TECH.MF.ZS or GB.XPD.RSDV.GD.ZS"}

        export_exposure = min(max((hitech or 0.0) / 60.0, 0.0), 1.0)
        rnd_vulnerability = min(max(1.0 - (rnd or 0.0) / 5.0, 0.0), 1.0)

        components = []
        if hitech is not None:
            components.append(export_exposure * 0.5)
        if rnd is not None:
            components.append(rnd_vulnerability * 0.5)

        score = float(min(max(sum(components) * 100, 0.0), 100.0))

        return {
            "score": round(score, 2),
            "hitech_export_share_pct": round(hitech, 2) if hitech is not None else None,
            "rnd_pct_gdp": round(rnd, 4) if rnd is not None else None,
            "export_decoupling_exposure": round(export_exposure, 4),
            "technology_import_reliance": round(rnd_vulnerability, 4),
            "metrics": {
                "hitech_indicator": _HITECH_CODE,
                "rnd_indicator": _RND_CODE,
            },
        }
