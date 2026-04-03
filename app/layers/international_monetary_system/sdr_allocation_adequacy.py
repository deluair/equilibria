"""SDR allocation adequacy: IMF Special Drawing Rights relative to global reserve needs.

SDRs serve as a supplementary international reserve asset. Adequacy is proxied
by comparing total foreign exchange reserves to GDP, since direct SDR allocation
data is not universally available in WDI. A low FX-to-GDP ratio signals that
the international liquidity backstop is thin; excessive concentration in a single
reserve currency amplifies systemic fragility.

Score: high reserves/GDP -> STABLE (liquidity buffer adequate), low reserves/GDP
-> rising stress, very low -> CRISIS (insufficient international liquidity).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SDRAllocationAdequacy(LayerBase):
    layer_id = "lMS"
    name = "SDR Allocation Adequacy"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "FI.RES.TOTL.CD"
        name = "Total reserves"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("NY.GDP.MKTP.CD", "%GDP%current%"),
        )

        res_vals = [r["value"] for r in rows if r["value"] is not None]
        gdp_vals = [r["value"] for r in gdp_rows if r["value"] is not None]

        if not res_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for FI.RES.TOTL.CD",
            }

        latest_res = res_vals[0]
        ratio = None
        if gdp_vals and gdp_vals[0] and gdp_vals[0] > 0:
            ratio = (latest_res / gdp_vals[0]) * 100.0

        # Score based on reserves/GDP ratio: lower ratio = more stress
        if ratio is None:
            score = 50.0
        elif ratio >= 30:
            score = 10.0
        elif ratio >= 20:
            score = 10.0 + (30 - ratio) * 2.0
        elif ratio >= 10:
            score = 30.0 + (20 - ratio) * 2.5
        elif ratio >= 5:
            score = 55.0 + (10 - ratio) * 3.0
        else:
            score = min(100.0, 70.0 + (5 - ratio) * 4.0)

        score = round(score, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "total_reserves_usd": round(latest_res, 0) if latest_res else None,
                "reserves_gdp_pct": round(ratio, 2) if ratio is not None else None,
                "n_obs": len(res_vals),
                "adequacy": (
                    "strong" if (ratio or 0) >= 20
                    else "moderate" if (ratio or 0) >= 10
                    else "weak"
                ),
            },
        }
