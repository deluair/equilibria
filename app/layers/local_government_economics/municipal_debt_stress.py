"""Municipal Debt Stress module.

Proxies municipal/subnational debt stress using central government debt-to-GDP
(GC.DOD.TOTL.GD.ZS) as a proxy. High national debt is strongly correlated with
constrained subnational borrowing space, fiscal transfers crowded out by debt
service, and elevated rollover risk for sub-sovereign entities.

Score reflects stress: high score = high municipal debt stress.
Score = clip(debt_gdp / 100 * 100, 0, 100), anchored at 100% debt-to-GDP = full stress.

Sources: WDI GC.DOD.TOTL.GD.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_CRITICAL_DEBT_THRESHOLD = 90.0  # % GDP, widely cited unsustainable threshold


class MunicipalDebtStress(LayerBase):
    layer_id = "lLG"
    name = "Municipal Debt Stress"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        code = "GC.DOD.TOTL.GD.ZS"
        name = "central government debt"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no central government debt data"}

        values = [float(r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid debt values"}

        debt_gdp = values[0]
        avg_debt = float(np.mean(values))

        # Stress rises linearly: 0% debt = 0 stress, 100% debt = 100 stress
        score = float(np.clip(debt_gdp, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "debt_pct_gdp": round(debt_gdp, 2),
            "avg_debt_pct_gdp_15yr": round(avg_debt, 2),
            "critical_threshold_pct_gdp": _CRITICAL_DEBT_THRESHOLD,
            "above_critical_threshold": debt_gdp > _CRITICAL_DEBT_THRESHOLD,
            "interpretation": (
                "Extreme municipal debt stress: subnational space severely constrained"
                if score > 75
                else "High debt stress: transfers and borrowing capacity at risk" if score > 50
                else "Moderate debt stress" if score > 30
                else "Low municipal debt stress"
            ),
            "_sources": ["WDI:GC.DOD.TOTL.GD.ZS"],
        }
