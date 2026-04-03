"""Remote work matching expansion: ICT skills penetration as geographic constraint reduction.

Remote work eliminates the geographic constraint in labor market matching,
enabling workers in depressed regions to access jobs in high-demand markets
without relocating. Countries with high ICT skills penetration and broadband
infrastructure can leverage remote work to dramatically expand effective labor
market reach, reducing structural regional mismatches.

Score: high ICT penetration (>70%) -> STABLE broad remote-work capability,
moderate (40-70%) -> WATCH partial remote capacity, low (15-40%) -> STRESS
limited remote matching potential, very low (<15%) -> CRISIS geography-bound
matching with no remote expansion.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class RemoteWorkMatchingExpansion(LayerBase):
    layer_id = "lLM"
    name = "Remote Work Matching Expansion"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        internet_code = "IT.NET.USER.ZS"
        broadband_code = "IT.NET.BBND.P2"
        ict_export_code = "TX.VAL.ICTG.ZS.UN"

        internet_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (internet_code, "%internet%users%"),
        )
        broadband_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (broadband_code, "%broadband%subscriptions%"),
        )
        ict_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ict_export_code, "%ICT.*export%"),
        )

        internet_vals = [r["value"] for r in internet_rows if r["value"] is not None]
        broadband_vals = [r["value"] for r in broadband_rows if r["value"] is not None]
        ict_vals = [r["value"] for r in ict_rows if r["value"] is not None]

        if not internet_vals and not broadband_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for internet/broadband penetration",
            }

        # Primary signal: internet penetration
        if internet_vals:
            ict_penetration = internet_vals[0]
            trend = round(internet_vals[0] - internet_vals[-1], 3) if len(internet_vals) > 1 else None
        else:
            # Broadband subscriptions per 100 as fallback
            ict_penetration = min(100.0, broadband_vals[0])
            trend = (
                round(broadband_vals[0] - broadband_vals[-1], 3)
                if len(broadband_vals) > 1
                else None
            )

        # Boost from fixed broadband (quality matters for remote work)
        broadband_boost = 0.0
        if broadband_vals:
            bb_rate = broadband_vals[0]
            if bb_rate > 30:
                broadband_boost = -10.0  # high broadband = lower friction score
            elif bb_rate > 10:
                broadband_boost = -5.0

        # Invert: higher ICT penetration = lower remote matching friction
        if ict_penetration >= 70:
            score = max(0.0, (100.0 - ict_penetration) * 0.4)
        elif ict_penetration >= 40:
            score = 12.0 + (70.0 - ict_penetration) * 0.8
        elif ict_penetration >= 15:
            score = 36.0 + (40.0 - ict_penetration) * 1.2
        else:
            score = min(100.0, 66.0 + (15.0 - ict_penetration) * 1.5)

        score = max(0.0, min(100.0, score + broadband_boost))

        # Trend adjustment: rapid ICT growth = improving remote capacity
        if trend is not None:
            if trend > 10:
                score = max(0.0, score - 6.0)
            elif trend > 5:
                score = max(0.0, score - 3.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "internet_penetration_pct": round(ict_penetration, 2),
                "broadband_per_100": round(broadband_vals[0], 2) if broadband_vals else None,
                "ict_export_share_pct": round(ict_vals[0], 2) if ict_vals else None,
                "ict_growth_trend_pct": trend,
                "n_obs_internet": len(internet_vals),
                "n_obs_broadband": len(broadband_vals),
                "remote_capacity": (
                    "high" if ict_penetration >= 70
                    else "moderate" if ict_penetration >= 40
                    else "low" if ict_penetration >= 15
                    else "minimal"
                ),
            },
        }
