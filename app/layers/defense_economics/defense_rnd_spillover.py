"""Defense R&D spillover: defense R&D contribution to civilian technological progress.

Defense R&D historically generated major civilian technology spillovers:
the internet (ARPANET), GPS, microwave ovens, and much of early semiconductor
development. This module proxies defense R&D spillover capacity using total
R&D expenditure as % GDP (GB.XPD.RSDV.GD.ZS) combined with high-technology
exports share (TX.VAL.TECH.MF.ZS) as an outcome indicator.

Higher R&D investment combined with strong tech exports suggests effective
knowledge spillover from public (including defense) R&D to the private sector.

Score: high R&D + high tech exports -> STABLE productive spillover,
low R&D + low tech exports -> STRESS knowledge gap.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DefenseRndSpillover(LayerBase):
    layer_id = "lDX"
    name = "Defense R&D Spillover"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        rnd_code = "GB.XPD.RSDV.GD.ZS"  # R&D expenditure % GDP
        tech_code = "TX.VAL.TECH.MF.ZS"  # High-tech exports % manufactured exports

        rnd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (rnd_code, "%research and development%GDP%"),
        )
        tech_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (tech_code, "%high-technology exports%"),
        )

        rnd_vals = [r["value"] for r in rnd_rows if r["value"] is not None]
        tech_vals = [r["value"] for r in tech_rows if r["value"] is not None]

        if not rnd_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for R&D expenditure GB.XPD.RSDV.GD.ZS",
            }

        rnd = rnd_vals[0]
        tech = tech_vals[0] if tech_vals else None

        # Spillover index: composite of R&D intensity and tech export outcome
        if tech is not None:
            spillover = (rnd * 20.0) + (tech * 0.5)  # weighted composite
        else:
            spillover = rnd * 20.0

        # Higher spillover = better (invert for scoring: lower score = STABLE)
        if spillover >= 50.0:
            score = 10.0
        elif spillover >= 30.0:
            score = 10.0 + (50.0 - spillover) * 0.75
        elif spillover >= 15.0:
            score = 25.0 + (30.0 - spillover) * 1.67
        elif spillover >= 5.0:
            score = 50.0 + (15.0 - spillover) * 2.0
        else:
            score = min(100.0, 70.0 + (5.0 - spillover) * 6.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "rnd_gdp_pct": round(rnd, 3),
                "high_tech_export_share_pct": round(tech, 3) if tech is not None else None,
                "spillover_index": round(spillover, 3),
                "n_obs_rnd": len(rnd_vals),
                "n_obs_tech": len(tech_vals),
                "spillover_capacity": (
                    "high" if spillover >= 50.0
                    else "moderate" if spillover >= 30.0
                    else "limited" if spillover >= 15.0
                    else "low"
                ),
            },
        }
