"""News desert spread: internet access gap as proxy for news access inequality.

News deserts -- communities with no local news coverage -- correlate strongly
with internet access gaps. Where fixed broadband penetration is low and rural
populations are large, residents have no digital substitute for vanishing local
newspapers. The access gap between urban and rural digital infrastructure
proxies for the geographic spread of news deserts.

Score: narrow access gap and high penetration -> STABLE; widening gap with
moderate penetration -> WATCH; large access gap -> STRESS; very low penetration
with large rural population -> CRISIS news desert conditions.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class NewsDesertSpread(LayerBase):
    layer_id = "lMD"
    name = "News Desert Spread"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        net_code = "IT.NET.USER.ZS"
        bb_code = "IT.NET.BBND.P2"
        rural_code = "SP.RUR.TOTL.ZS"

        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (net_code, "%internet users%"),
        )
        bb_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (bb_code, "%broadband%"),
        )
        rural_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rural_code, "%rural population%"),
        )

        net_vals = [r["value"] for r in net_rows if r["value"] is not None]
        bb_vals = [r["value"] for r in bb_rows if r["value"] is not None]
        rural_vals = [r["value"] for r in rural_rows if r["value"] is not None]

        if not net_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for internet users IT.NET.USER.ZS",
            }

        net_pct = net_vals[0]
        bb_per100 = bb_vals[0] if bb_vals else None
        rural_pct = rural_vals[0] if rural_vals else 30.0  # global avg fallback

        # Access gap: difference between overall internet use and fixed broadband
        # Large gap means mobile-only connectivity (shallow, costly, unreliable)
        access_gap = (net_pct - bb_per100) if bb_per100 is not None else net_pct * 0.5

        # News desert risk composite
        # High rural population with low broadband -> severe desert conditions
        desert_risk = (rural_pct / 100.0) * max(0, 100 - net_pct) / 100.0

        # Score: blend of absolute penetration gap and rural exposure
        if net_pct >= 80 and access_gap < 30:
            base = 15.0
        elif net_pct >= 60:
            base = 25.0 + access_gap * 0.3
        elif net_pct >= 40:
            base = 40.0 + access_gap * 0.4
        else:
            base = 55.0 + (100 - net_pct) * 0.3

        # Rural weight amplifier
        base = min(100.0, base + desert_risk * 20.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "internet_users_pct": round(net_pct, 2),
                "broadband_per_100": round(bb_per100, 2) if bb_per100 is not None else None,
                "rural_population_pct": round(rural_pct, 2),
                "access_gap": round(access_gap, 2),
                "desert_risk_index": round(desert_risk, 4),
                "n_obs_internet": len(net_vals),
                "n_obs_broadband": len(bb_vals),
                "n_obs_rural": len(rural_vals),
            },
        }
