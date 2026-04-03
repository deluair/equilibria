"""Competitive Dynamics module.

Measures competitive turbulence through cross-sector growth rate volatility.
Rapidly shifting sector shares indicate creative destruction and competitive
pressure; extreme volatility may also signal structural instability.

Method:
- Collect time-series of manufacturing VA % GDP (NV.IND.MANF.ZS) and
  services VA % GDP (NV.SRV.TOTL.ZS).
- Compute year-on-year changes for each series.
- Cross-sector standard deviation = std of changes across sectors per period,
  then averaged over time.
- High cross-sector volatility = high competitive turbulence.

Score = clip(cross_sector_std * 10, 0, 100).

Sources: WDI (NV.IND.MANF.ZS, NV.SRV.TOTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_DYNAMIC_SERIES = ["NV.IND.MANF.ZS", "NV.SRV.TOTL.ZS"]


class CompetitiveDynamics(LayerBase):
    layer_id = "lCO"
    name = "Competitive Dynamics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        placeholders = ", ".join("?" * len(_DYNAMIC_SERIES))
        rows = await db.fetch_all(
            f"""
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ({placeholders})
              AND dp.value IS NOT NULL
            ORDER BY ds.series_id, dp.date
            """,
            (country, *_DYNAMIC_SERIES),
        )

        if not rows or len(rows) < 6:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient sector time series"}

        # Group by series
        series_data: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            sid = r["series_id"]
            try:
                val = float(r["value"])
            except (TypeError, ValueError):
                continue
            series_data.setdefault(sid, []).append((r["date"], val))

        if len(series_data) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "need at least 2 sector series"}

        # Compute YoY changes per series
        changes_per_series: dict[str, np.ndarray] = {}
        for sid, pairs in series_data.items():
            pairs_sorted = sorted(pairs, key=lambda x: x[0])
            vals = np.array([v for _, v in pairs_sorted])
            if len(vals) >= 3:
                changes_per_series[sid] = np.diff(vals)

        if len(changes_per_series) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient changes data"}

        # Align lengths
        min_len = min(len(c) for c in changes_per_series.values())
        aligned = np.array([c[-min_len:] for c in changes_per_series.values()])

        # Cross-sector std at each time step, then mean
        cross_std_per_period = np.std(aligned, axis=0)
        mean_cross_std = float(np.mean(cross_std_per_period))

        score = float(np.clip(mean_cross_std * 10, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "mean_cross_sector_std": round(mean_cross_std, 4),
            "n_periods": int(min_len),
            "series_used": list(changes_per_series.keys()),
            "interpretation": (
                "low turbulence (stable sectors)" if score < 33
                else "moderate competitive dynamics" if score < 66
                else "high competitive turbulence"
            ),
            "reference": "Schumpeter (1942): creative destruction; industry dynamics literature",
        }
