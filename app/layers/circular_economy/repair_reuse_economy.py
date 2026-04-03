"""Repair and reuse economy: services employment as proxy for repair sector.

Services value added as a share of GDP (NV.SRV.TOTL.ZS) proxies the size of
the repair, maintenance, and reuse economy. Economies with larger services
sectors tend to have stronger circular economy activities including repair,
rental, refurbishment, and second-hand markets relative to linear production.

References:
    Stahel, W.R. (2010). The Performance Economy. Palgrave Macmillan.
    Ellen MacArthur Foundation (2013). Towards the Circular Economy Vol. 1.
    World Bank WDI: NV.SRV.TOTL.ZS
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RepairReuseEconomy(LayerBase):
    layer_id = "lCE"
    name = "Repair Reuse Economy"

    SERVICES_CODE = "NV.SRV.TOTL.ZS"

    async def compute(self, db, **kwargs) -> dict:
        svc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.SERVICES_CODE, f"%{self.SERVICES_CODE}%"),
        )

        if not svc_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no services value added data for repair/reuse economy",
            }

        svc_vals = [r["value"] for r in svc_rows if r["value"] is not None]
        if not svc_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "null services value added values",
            }

        svc_latest = float(svc_vals[0])

        # Trend in services share (rising = growing repair/reuse capacity)
        svc_trend = None
        if len(svc_vals) >= 3:
            arr = np.array(svc_vals[:10], dtype=float)
            svc_trend = float(np.polyfit(np.arange(len(arr)), arr, 1)[0])

        # Score: low services share = weaker repair/reuse economy = higher CE stress
        # Global average services ~60% of GDP; high-income ~75%
        benchmark = 60.0
        gap = max(0.0, benchmark - svc_latest)
        raw_score = min(gap * 2.0, 100.0)

        # Reward improving trend
        if svc_trend is not None and svc_trend > 0:
            raw_score = max(0.0, raw_score - svc_trend * 5.0)

        score = float(np.clip(raw_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "services_value_added_pct_gdp": round(svc_latest, 2),
            "services_trend_slope_pp_yr": round(svc_trend, 4) if svc_trend is not None else None,
            "benchmark_services_pct_gdp": benchmark,
            "services_gap_pp": round(gap, 2),
        }
