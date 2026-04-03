"""Critical mineral concentration: export concentration in critical minerals.

Critical minerals (lithium, cobalt, rare earths, copper, nickel) underpin the
energy transition. Countries with highly concentrated exports in these minerals
face Dutch disease risks, while importers face supply chain bottlenecks.
Proxied via WDI TX.VAL.MMTL.ZS (ores and metals % of merchandise exports)
as a structural indicator of mineral export concentration.

Score: diversified economy (low mineral export share) with stable trend -> STABLE,
high concentration -> WATCH/STRESS due to commodity cycle vulnerability.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CriticalMineralConcentration(LayerBase):
    layer_id = "lES"
    name = "Critical Mineral Concentration"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "TX.VAL.MMTL.ZS"
        name = "Ores and metals exports"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for TX.VAL.MMTL.ZS"}

        vals = [r["value"] for r in rows if r["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = vals[0]
        trend = round(vals[0] - vals[-1], 3) if len(vals) > 1 else None

        # Concentration risk: very high mineral export share indicates vulnerability
        # to commodity price cycles and Dutch disease effects.
        # For energy security, it also signals whether a country controls key inputs
        # for transition (high = strategic asset) vs. lacks diversification (high = risk).
        # Score reflects export concentration risk (not strategic value).
        if latest < 5:
            score = 8.0 + latest * 2.0
        elif latest < 20:
            score = 18.0 + (latest - 5) * 1.47
        elif latest < 40:
            score = 40.0 + (latest - 20) * 1.0
        elif latest < 70:
            score = 60.0 + (latest - 40) * 0.67
        else:
            score = min(100.0, 80.0 + (latest - 70) * 0.67)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "mineral_exports_pct_merchandise": round(latest, 2),
                "trend_pct_change": trend,
                "n_obs": len(vals),
                "concentration_tier": (
                    "diversified" if latest < 5
                    else "low_concentration" if latest < 20
                    else "moderate" if latest < 40
                    else "high" if latest < 70
                    else "critical"
                ),
            },
        }
