"""Asset Quality Ratio (NPL trend).

Rising non-performing loan ratios signal deteriorating bank asset quality.
Queries NPL-related series by description keyword match, with fallback to
the WDI series FB.AST.NPER.ZS.

Score (0-100): clip(npl_pct * 8, 0, 100).
NPL > 12.5% maps to CRISIS.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class AssetQualityRatio(LayerBase):
    layer_id = "lMP"
    name = "Asset Quality Ratio"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        # Primary: keyword search in description
        rows = await db.fetch_all(
            """
            SELECT ds.series_code, ds.description, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
              AND (LOWER(ds.description) LIKE '%npl%'
                   OR LOWER(ds.description) LIKE '%nonperform%'
                   OR LOWER(ds.description) LIKE '%non_perform%'
                   OR ds.series_code = 'FB.AST.NPER.ZS')
            ORDER BY dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no NPL data found",
            }

        series_map: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            key = r["series_code"]
            series_map.setdefault(key, []).append((r["date"], float(r["value"])))

        # Use first available series
        chosen_key = next(iter(series_map))
        chosen_series = series_map[chosen_key]
        values = [v[1] for v in chosen_series]
        dates = [v[0] for v in chosen_series]
        latest_npl = values[-1]

        score = float(np.clip(latest_npl * 8.0, 0.0, 100.0))

        trend = None
        if len(values) >= 3:
            x = np.arange(len(values))
            slope, _, r_val, _, _ = sp_stats.linregress(x, values)
            trend = {
                "slope_per_year": round(float(slope), 4),
                "r_squared": round(float(r_val ** 2), 4),
                "direction": "rising" if slope > 0.2 else "falling" if slope < -0.2 else "stable",
            }

        return {
            "score": round(score, 2),
            "country": country,
            "npl_ratio_pct": round(latest_npl, 2),
            "series_used": chosen_key,
            "observations": len(values),
            "trend": trend,
            "date_range": {"start": dates[0], "end": dates[-1]},
            "interpretation": self._interpret(latest_npl, trend),
        }

    @staticmethod
    def _interpret(npl: float, trend: dict | None) -> str:
        direction = trend["direction"] if trend else "unknown"
        if npl > 15:
            return f"severe asset quality deterioration: NPL {npl:.1f}% ({direction})"
        if npl > 10:
            return f"high NPL ratio: {npl:.1f}% - regulatory action warranted ({direction})"
        if npl > 5:
            return f"elevated NPL ratio: {npl:.1f}% ({direction})"
        return f"NPL ratio within normal range: {npl:.1f}% ({direction})"
