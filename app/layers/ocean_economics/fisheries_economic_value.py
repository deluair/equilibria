"""Fisheries economic value: capture fish production trend as proxy for fisheries GDP share.

Uses agriculture value added (NV.AGR.TOTL.ZS) and aquaculture proxy to estimate
the economic value of fisheries. A declining agriculture share or stagnating
capture trend signals rising fisheries vulnerability.

Sources: World Bank WDI (NV.AGR.TOTL.ZS), FAO FishStat
"""

from __future__ import annotations

from app.layers.base import LayerBase


class FisheriesEconomicValue(LayerBase):
    layer_id = "lOE"
    name = "Fisheries Economic Value"

    async def compute(self, db, **kwargs) -> dict:
        code = "NV.AGR.TOTL.ZS"
        name = "agriculture value added"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No fisheries/agriculture data found",
            }

        values = [row["value"] for row in rows if row["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All fetched rows have null values",
            }

        latest = float(values[0])
        mean_val = sum(values) / len(values)

        # Fish production ~15% of agriculture value added (FAO baseline)
        fish_pct_gdp = latest * 0.15

        # Trend: compare latest vs mean (declining = worsening)
        trend = "stable"
        if len(values) >= 3:
            recent = sum(values[:3]) / 3
            older = sum(values[-3:]) / 3
            if recent < older * 0.95:
                trend = "declining"
            elif recent > older * 1.05:
                trend = "growing"

        # Score: higher fish GDP share = lower vulnerability (inverse)
        # 0% fish GDP -> score 80 (crisis), 5%+ -> score 10 (stable)
        raw_score = max(0.0, 80.0 - fish_pct_gdp * 14.0)
        score = round(min(100.0, raw_score), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "agriculture_value_added_pct_gdp": round(latest, 2),
                "fish_gdp_pct_estimate": round(fish_pct_gdp, 3),
                "mean_agriculture_pct": round(mean_val, 2),
                "trend": trend,
                "n_obs": len(values),
            },
        }
