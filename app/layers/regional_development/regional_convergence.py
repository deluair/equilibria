"""Regional Convergence module.

Measures income convergence across regions by proxying via GDP per capita
vs world average (frontier). A persistent large gap indicates regional
divergence -- the country's income frontier is far from the world leader,
implying internal regions are even further behind.

Score = clip((gap_pct - 20) * 1.25, 0, 100)
where gap_pct is the percentage shortfall from world frontier GDP per capita.
A 20 % gap or less is treated as near-convergence (score ~0); an 80 % or
larger gap maps to the maximum stress score.

Sources: WDI NY.GDP.PCAP.KD (constant 2015 USD, country + world average)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RegionalConvergence(LayerBase):
    layer_id = "lRD"
    name = "Regional Convergence"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_country = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        rows_world = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = 'WLD'
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
        )

        if not rows_country or not rows_world:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        country_vals = {r["date"]: float(r["value"]) for r in rows_country if r["value"] is not None}
        world_vals = {r["date"]: float(r["value"]) for r in rows_world if r["value"] is not None}

        common_dates = sorted(set(country_vals) & set(world_vals), reverse=True)
        if not common_dates:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping dates"}

        gaps = []
        for d in common_dates:
            c = country_vals[d]
            w = world_vals[d]
            if w > 0:
                gap_pct = max(0.0, (w - c) / w * 100)
                gaps.append(gap_pct)

        if not gaps:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid gap observations"}

        mean_gap = float(np.mean(gaps))
        latest_gap = gaps[0]

        score = float(np.clip((mean_gap - 20) * 1.25, 0, 100))

        trend = None
        if len(gaps) >= 3:
            x = np.arange(len(gaps), dtype=float)
            slope = float(np.polyfit(x, gaps, 1)[0])
            trend = "diverging" if slope > 0.5 else ("converging" if slope < -0.5 else "stable")

        return {
            "score": round(score, 1),
            "country": country,
            "latest_date": common_dates[0],
            "latest_gap_pct": round(latest_gap, 2),
            "mean_gap_pct": round(mean_gap, 2),
            "n_obs": len(gaps),
            "trend": trend,
            "series": "NY.GDP.PCAP.KD",
            "frontier": "WLD (world average)",
        }
