"""Resource Geopolitics Risk: natural resource rents + import dependence.

Countries with high natural resource rents (NY.GDP.TOTL.RT.ZS) face the
"resource curse" and geopolitical targeting. High import shares (NE.IMP.GNFS.ZS)
combined with resource dependence creates dual exposure: both a prize and a
dependency. Energy import share is proxied from total imports.

Methodology:
    resource_rents = latest NY.GDP.TOTL.RT.ZS (total natural resource rents % GDP)
    import_gdp = latest NE.IMP.GNFS.ZS (imports of goods and services % GDP)

    resource_score = clip(resource_rents / 50.0, 0, 1)  -- 50% GDP = max reference
    import_score = clip(import_gdp / 100.0, 0, 1)       -- 100% GDP = max reference
    score = clip((resource_score * 0.55 + import_score * 0.45) * 100, 0, 100)

Score (0-100): Higher = greater resource geopolitics risk.

References:
    Ross, M. (2012). The Oil Curse. Princeton UP.
    Colgan, J. (2013). Petro-Aggression. Cambridge UP.
    World Bank WDI NY.GDP.TOTL.RT.ZS, NE.IMP.GNFS.ZS.
"""

from __future__ import annotations

from app.layers.base import LayerBase

_RESOURCE_CODE = "NY.GDP.TOTL.RT.ZS"
_IMPORT_CODE = "NE.IMP.GNFS.ZS"


class ResourceGeopoliticsRisk(LayerBase):
    layer_id = "lGP"
    name = "Resource Geopolitics Risk"

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
        resource_rents = await self._fetch_latest(db, _RESOURCE_CODE, "natural resource rents")
        import_gdp = await self._fetch_latest(db, _IMPORT_CODE, "imports goods services")

        if resource_rents is None and import_gdp is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for NY.GDP.TOTL.RT.ZS or NE.IMP.GNFS.ZS"}

        resource_score = min(max((resource_rents or 0.0) / 50.0, 0.0), 1.0)
        import_score = min(max((import_gdp or 0.0) / 100.0, 0.0), 1.0)

        components = []
        if resource_rents is not None:
            components.append(resource_score * 0.55)
        if import_gdp is not None:
            components.append(import_score * 0.45)

        score = float(min(max(sum(components) * 100, 0.0), 100.0))

        return {
            "score": round(score, 2),
            "natural_resource_rents_pct_gdp": round(resource_rents, 2) if resource_rents is not None else None,
            "imports_pct_gdp": round(import_gdp, 2) if import_gdp is not None else None,
            "resource_dependence_score": round(resource_score, 4),
            "import_exposure_score": round(import_score, 4),
            "metrics": {
                "resource_indicator": _RESOURCE_CODE,
                "import_indicator": _IMPORT_CODE,
            },
        }
