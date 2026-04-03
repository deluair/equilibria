"""Nonprofit sector size: employment share as proxy for sector scale.

Nonprofit employment as a share of total employment captures the economic
footprint of civil society. Proxied via WDI social indicators including
public/government employment and social protection coverage, which correlate
with the formal nonprofit ecosystem scale in an economy.

Score: very low share (<2%) -> STABLE nascent sector, moderate (2-5%) ->
WATCH developing, substantial (5-10%) -> STRESS mature with resource pressure,
high (>10%) -> CRISIS over-reliance or unsustainable dependency.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class NonprofitSectorSize(LayerBase):
    layer_id = "lNP"
    name = "Nonprofit Sector Size"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "SL.EMP.WORK.ZS"
        name = "wage and salaried workers"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SL.EMP.WORK.ZS"}

        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        latest = values[0]
        trend = round(values[0] - values[-1], 3) if len(values) > 1 else None

        # Nonprofit sector proxy: formal wage employment share correlates with
        # civil society institutional capacity. Higher formal employment = larger
        # potential nonprofit labor pool. Score reflects sector development pressure.
        if latest < 20:
            score = 15.0 + latest * 0.5
        elif latest < 40:
            score = 25.0 + (latest - 20) * 0.75
        elif latest < 65:
            score = 40.0 + (latest - 40) * 0.8
        else:
            score = min(100.0, 60.0 + (latest - 65) * 1.1)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "wage_employment_share_pct": round(latest, 2),
                "trend_pct_change": trend,
                "n_obs": len(values),
                "sector_stage": (
                    "nascent" if latest < 20
                    else "developing" if latest < 40
                    else "mature" if latest < 65
                    else "advanced"
                ),
            },
        }
