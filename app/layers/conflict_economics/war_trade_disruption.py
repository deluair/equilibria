"""War Trade Disruption module.

Measures trade flow disruption from conflict using deviation of trade openness
(NE.TRD.GNFS.ZS) from its own long-run trend. Sudden drops in trade openness
in fragile states signal conflict-driven disruption. Severity is amplified by
export concentration (TX.VAL.MRCH.XD.WD volatility).

Score = clip(disruption_index * 100, 0, 100).
High score = severe trade disruption attributable to conflict.

Sources: WDI (NE.TRD.GNFS.ZS, TX.VAL.MRCH.XD.WD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WarTradeDisruption(LayerBase):
    layer_id = "lCW"
    name = "War Trade Disruption"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        trade_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date
            LIMIT 30
            """,
            (country,),
        )

        export_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'TX.VAL.MRCH.XD.WD'
            ORDER BY dp.date
            LIMIT 20
            """,
            (country,),
        )

        if not trade_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        trade_vals = [float(r["value"]) for r in trade_rows if r["value"] is not None]
        if len(trade_vals) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        arr = np.array(trade_vals)

        # Trend deviation: gap between current and long-run average
        trend_mean = float(np.mean(arr))
        recent_mean = float(np.mean(arr[-5:])) if len(arr) >= 5 else float(arr[-1])
        pct_drop = (trend_mean - recent_mean) / trend_mean if trend_mean > 0 else 0.0
        pct_drop = max(pct_drop, 0.0)  # Only penalize drops

        # Volatility component
        trade_std = float(np.std(arr))
        volatility_cv = trade_std / trend_mean if trend_mean > 0 else 0.0

        # Export volatility as amplifier
        export_vals = [float(r["value"]) for r in export_rows if r["value"] is not None]
        export_volatility = float(np.std(export_vals) / np.mean(export_vals)) if len(export_vals) >= 3 and np.mean(export_vals) > 0 else None

        drop_component = float(np.clip(pct_drop * 150, 0, 60))
        vol_component = float(np.clip(volatility_cv * 80, 0, 30))
        export_component = float(np.clip(export_volatility * 20, 0, 10)) if export_volatility is not None else 0.0

        score = float(np.clip(drop_component + vol_component + export_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "trade_openness_long_run_mean": round(trend_mean, 4),
            "trade_openness_recent_mean": round(recent_mean, 4),
            "pct_drop_from_trend": round(pct_drop, 4),
            "trade_openness_cv": round(volatility_cv, 4),
            "export_price_volatility_cv": round(export_volatility, 4) if export_volatility is not None else None,
            "drop_component": round(drop_component, 2),
            "vol_component": round(vol_component, 2),
            "export_component": round(export_component, 2),
            "n_obs": len(trade_vals),
            "indicators": {
                "trade_openness": "NE.TRD.GNFS.ZS",
                "export_price_index": "TX.VAL.MRCH.XD.WD",
            },
        }
