"""Child Poverty Rate module.

Estimates child poverty exposure by combining the $2.15/day headcount
(SI.POV.DDAY) with the child dependency ratio (SP.POP.DPND.YG -- dependents
0-14 as % of working-age population). A high headcount combined with a high
dependency ratio implies a disproportionate poverty burden on children.

Score = clip(headcount * (1 + dependency_ratio / 100), 0, 100).

Sources: WDI (SI.POV.DDAY, SP.POP.DPND.YG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ChildPovertyRate(LayerBase):
    layer_id = "lPM"
    name = "Child Poverty Rate"

    async def compute(self, db, **kwargs) -> dict:
        hc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SI.POV.DDAY", "%poverty headcount%"),
        )
        dep_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("SP.POP.DPND.YG", "%age dependency ratio young%"),
        )

        hc_vals = [float(r["value"]) for r in hc_rows if r["value"] is not None] if hc_rows else []
        dep_vals = [float(r["value"]) for r in dep_rows if r["value"] is not None] if dep_rows else []

        if not hc_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SI.POV.DDAY"}

        headcount = hc_vals[0]
        dependency = dep_vals[0] if dep_vals else None

        if dependency is not None:
            raw = headcount * (1 + dependency / 100)
        else:
            raw = headcount * 1.5  # conservative uplift without dependency data

        score = float(np.clip(raw, 0, 100))

        return {
            "score": round(score, 1),
            "headcount_pct": round(headcount, 3),
            "child_dependency_ratio": round(dependency, 2) if dependency is not None else None,
            "child_poverty_proxy": round(raw, 2),
            "n_obs_headcount": len(hc_vals),
            "n_obs_dependency": len(dep_vals),
            "methodology": "headcount * (1 + dependency_ratio/100)",
        }
