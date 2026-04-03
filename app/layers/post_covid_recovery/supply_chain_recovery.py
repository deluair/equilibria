"""Supply chain recovery: merchandise trade recovery to pre-pandemic trend.

Global merchandise trade collapsed ~5.3% in 2020 (WTO), then rebounded sharply
in 2021. However, persistent port congestion, container shortages, semiconductor
gaps, and energy disruptions kept supply chains under stress through 2022-2023.
Recovery is assessed by comparing merchandise trade growth against the pre-2020
trend (WDI NE.TRD.GNFS.ZS: trade % of GDP).

Full recovery means returning to the pre-pandemic trend trajectory. Ongoing
deviations below trend signal structural fragmentation or demand displacement.

Score: close to or above trend (gap <5%) -> STABLE, moderate gap (5-15%) -> WATCH,
large gap (15-30%) -> STRESS, severe gap (>30%) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SupplyChainRecovery(LayerBase):
    layer_id = "lPC"
    name = "Supply Chain Recovery"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "NE.TRD.GNFS.ZS"
        name = "trade % of GDP"
        rows = await db.fetch_all(
            "SELECT value, date FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for NE.TRD.GNFS.ZS",
            }

        values = [(r["date"], r["value"]) for r in rows if r["value"] is not None]
        if len(values) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient observations"}

        values.sort(key=lambda x: x[0])

        pre_covid = [v for d, v in values if d < "2020-01-01"]
        post_covid = [v for d, v in values if d >= "2020-01-01"]

        if not pre_covid or not post_covid:
            all_vals = [v for _, v in values]
            pre_covid = all_vals[:len(all_vals) // 2]
            post_covid = all_vals[len(all_vals) // 2:]

        pre_avg = sum(pre_covid) / len(pre_covid) if pre_covid else None
        latest = post_covid[-1] if post_covid else values[-1][1]

        if pre_avg is None or pre_avg == 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "cannot compute pre-pandemic baseline"}

        # Gap: how far is current trade/GDP from pre-pandemic average
        gap_pct = max(0.0, (pre_avg - latest) / pre_avg * 100)

        if gap_pct < 5:
            score = 5.0 + gap_pct * 2.0
        elif gap_pct < 15:
            score = 15.0 + (gap_pct - 5) * 3.0
        elif gap_pct < 30:
            score = 45.0 + (gap_pct - 15) * 2.0
        else:
            score = min(100.0, 75.0 + (gap_pct - 30) * 0.83)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "latest_trade_gdp_pct": round(latest, 2),
                "pre_pandemic_avg_pct": round(pre_avg, 2),
                "recovery_gap_pct": round(gap_pct, 2),
                "above_pre_pandemic": latest >= pre_avg,
                "n_pre_obs": len(pre_covid),
                "n_post_obs": len(post_covid),
            },
        }
