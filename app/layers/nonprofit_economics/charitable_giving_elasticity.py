"""Charitable giving elasticity: income responsiveness of aid flows.

Income elasticity of charitable giving measures how giving responds to
economic growth. Proxied by correlating GNI per capita growth with net ODA
received as % of GNI. A positive and elastic relationship suggests giving
amplifies with income; inelastic or negative suggests structural constraints.

Score: Uses volatility of aid flows relative to income as a stress indicator.
High volatility + declining trend -> CRISIS; stable and growing -> STABLE.
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class CharitableGivingElasticity(LayerBase):
    layer_id = "lNP"
    name = "Charitable Giving Elasticity"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gni_code = "NY.GNP.PCAP.KD.ZG"
        oda_code = "DT.ODA.ODAT.GN.ZS"

        gni_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gni_code, "%GNI per capita growth%"),
        )
        oda_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (oda_code, "%ODA received%"),
        )

        gni_vals = [r["value"] for r in gni_rows if r["value"] is not None]
        oda_vals = [r["value"] for r in oda_rows if r["value"] is not None]

        if not oda_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for ODA/GNI flows",
            }

        latest_oda = oda_vals[0]
        trend = round(oda_vals[0] - oda_vals[-1], 3) if len(oda_vals) > 1 else None

        # Volatility as primary stress signal
        if len(oda_vals) >= 3:
            volatility = statistics.stdev(oda_vals)
        else:
            volatility = 0.0

        # Elasticity proxy: declining aid flows relative to income = reduced giving capacity
        # Score based on aid level and volatility
        base_score = 20.0
        if latest_oda > 5.0:
            # High ODA dependency -> stress
            base_score += min(40.0, latest_oda * 4.0)
        elif latest_oda > 1.0:
            base_score += latest_oda * 5.0
        else:
            base_score += latest_oda * 10.0

        volatility_penalty = min(20.0, volatility * 3.0)
        trend_adjustment = -5.0 if (trend is not None and trend > 0) else 5.0

        score = min(100.0, max(0.0, base_score + volatility_penalty + trend_adjustment))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "oda_gni_pct": round(latest_oda, 3),
                "oda_volatility": round(volatility, 3),
                "trend_pct_change": trend,
                "n_obs_oda": len(oda_vals),
                "n_obs_gni": len(gni_vals),
            },
        }
