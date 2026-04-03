"""Evolutionary Dynamics module.

Measures structural transformation speed via evolutionary game theory
(Maynard Smith & Price 1973, Young 1993 on evolutionary equilibria).

Rapid, volatile shifts in sector shares (agriculture, industry, services)
signal evolutionary pressure: the economy has not settled into a stable
population dynamic. High variance of annual changes in sector composition
= disruptive evolutionary pressure.

Score = normalized variance of annual sector share changes across all three sectors.

Sources: WDI (NV.AGR.TOTL.ZS, NV.IND.TOTL.ZS, NV.SRV.TOTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_SERIES = ["NV.AGR.TOTL.ZS", "NV.IND.TOTL.ZS", "NV.SRV.TOTL.ZS"]
_NAMES = ["agriculture", "industry", "services"]


class EvolutionaryDynamics(LayerBase):
    layer_id = "lGT"
    name = "Evolutionary Dynamics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        sector_data = {}
        for sid in _SERIES:
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date
                """,
                (country, sid),
            )
            if rows:
                sector_data[sid] = {r["date"]: float(r["value"]) for r in rows}

        if len(sector_data) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data: need at least 2 of NV.AGR.TOTL.ZS, NV.IND.TOTL.ZS, NV.SRV.TOTL.ZS",
            }

        # Common dates across available sectors
        common = sorted(set.intersection(*[set(v.keys()) for v in sector_data.values()]))

        if len(common) < 6:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "fewer than 6 common dates across sector series",
            }

        # Compute year-over-year absolute changes for each sector
        all_changes = []
        sector_stats = {}
        for sid, name in zip(_SERIES, _NAMES):
            if sid not in sector_data:
                continue
            vals = np.array([sector_data[sid][d] for d in common])
            changes = np.abs(np.diff(vals))
            all_changes.extend(changes.tolist())
            sector_stats[name] = {
                "mean_share": round(float(np.mean(vals)), 3),
                "mean_abs_change": round(float(np.mean(changes)), 4),
                "std_change": round(float(np.std(changes)), 4),
            }

        if not all_changes:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no valid annual changes computable",
            }

        all_arr = np.array(all_changes)
        mean_change = float(np.mean(all_arr))
        variance_change = float(np.var(all_arr))

        # Score: mean absolute annual change > 1 pp is moderate, > 5 pp is high
        # Scale: 0 = stable, 100 = highly disruptive (mean change >= 5 pp)
        score = float(np.clip(mean_change / 5.0 * 100.0, 0.0, 100.0))

        # Variance bonus: high variance amplifies score slightly
        variance_bonus = float(np.clip(np.sqrt(variance_change) / 3.0 * 20.0, 0.0, 20.0))
        score = float(np.clip(score + variance_bonus, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "mean_abs_annual_change_pp": round(mean_change, 4),
            "variance_of_changes": round(variance_change, 6),
            "n_common_dates": len(common),
            "n_sectors": len(sector_data),
            "period": f"{common[0]} to {common[-1]}",
            "sector_stats": sector_stats,
            "interpretation": (
                "highly disruptive structural transformation" if score > 60
                else "moderate evolutionary pressure" if score > 30
                else "stable evolutionary equilibrium in sector composition"
            ),
        }
