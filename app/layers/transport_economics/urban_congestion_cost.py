"""Urban Congestion Cost module.

Estimates congestion pressure using urbanization rate and population density
as joint proxies. High urbanization combined with high density without
commensurate infrastructure investment implies severe congestion costs.

Indicators: SP.URB.TOTL.IN.ZS (urban population % of total),
            EN.POP.DNST (population density, people per sq km).
Score = clip((urban_pct / 100 * density_factor) * 100, 0, 100)
where density_factor = clip(density / 500, 0, 1).

Sources: WDI SP.URB.TOTL.IN.ZS, EN.POP.DNST
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_DENSITY_THRESHOLD = 500.0  # people/km2 = high congestion pressure threshold


class UrbanCongestionCost(LayerBase):
    layer_id = "lTR"
    name = "Urban Congestion Cost"

    async def compute(self, db, **kwargs) -> dict:
        urb_code = "SP.URB.TOTL.IN.ZS"
        dens_code = "EN.POP.DNST"

        urb_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (urb_code, f"%{urb_code}%"),
        )
        dens_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (dens_code, f"%{dens_code}%"),
        )

        if not urb_rows and not dens_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SP.URB.TOTL.IN.ZS or EN.POP.DNST"}

        metrics: dict = {}
        score_components = []

        if urb_rows:
            urb_pct = float(urb_rows[0]["value"])
            metrics["urbanization_pct"] = round(urb_pct, 2)
            score_components.append(urb_pct)

        if dens_rows:
            density = float(dens_rows[0]["value"])
            density_factor = float(np.clip(density / _DENSITY_THRESHOLD, 0, 1))
            metrics["population_density"] = round(density, 2)
            metrics["density_factor"] = round(density_factor, 4)
            score_components.append(density_factor * 100.0)

        if urb_rows and dens_rows:
            urb_pct = metrics["urbanization_pct"]
            density_factor = metrics["density_factor"]
            score = float(np.clip(urb_pct / 100.0 * density_factor * 100.0, 0, 100))
        else:
            score = float(np.mean(score_components))

        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 1),
            "signal": self.classify_signal(score),
            "metrics": metrics,
            "_sources": ["WDI:SP.URB.TOTL.IN.ZS", "WDI:EN.POP.DNST"],
        }
