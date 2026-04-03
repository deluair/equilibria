"""Waste trade dependency: reliance on waste imports/exports as circular resource proxy.

Uses merchandise trade openness (NE.TRD.GNFS.ZS) and industrial CO2 intensity
(EN.CO2.MANF.ZS) as proxies for an economy's dependence on traded waste/scrap
materials. Economies with high trade openness and high industrial share often
rely more on cross-border waste/secondary material flows.

References:
    Basel Convention (2021). Technical Guidelines on Transboundary Movements of
        Hazardous Wastes and Other Wastes.
    OECD (2020). Global Plastics Outlook: Economic Drivers, Environmental Impacts
        and Policy Options.
    World Bank WDI: NE.TRD.GNFS.ZS, EN.CO2.MANF.ZS
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WasteTradeDependency(LayerBase):
    layer_id = "lCE"
    name = "Waste Trade Dependency"

    TRADE_CODE = "NE.TRD.GNFS.ZS"   # Trade (% of GDP)
    IND_CO2_CODE = "EN.CO2.MANF.ZS"  # Manufacturing CO2 (% of total fuel combustion)

    async def compute(self, db, **kwargs) -> dict:
        trade_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.TRADE_CODE, f"%{self.TRADE_CODE}%"),
        )
        ind_co2_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.IND_CO2_CODE, f"%{self.IND_CO2_CODE}%"),
        )

        if not trade_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no trade data for waste trade dependency",
            }

        trade_vals = [r["value"] for r in trade_rows if r["value"] is not None]
        if not trade_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "null trade values for waste trade dependency",
            }

        trade_latest = float(trade_vals[0])
        ind_co2_latest = None
        if ind_co2_rows:
            ind_co2_vals = [r["value"] for r in ind_co2_rows if r["value"] is not None]
            if ind_co2_vals:
                ind_co2_latest = float(ind_co2_vals[0])

        # Composite dependency index: trade openness weighted by industrial CO2 share
        # Higher trade + higher industrial CO2 = higher waste trade dependency
        if ind_co2_latest is not None and ind_co2_latest > 0:
            dependency_index = (trade_latest / 100.0) * (ind_co2_latest / 100.0) * 100.0
        else:
            dependency_index = trade_latest * 0.25  # fallback: trade alone, scaled

        # Trend in trade openness
        if len(trade_vals) >= 3:
            arr = np.array(trade_vals[:10], dtype=float)
            trend_slope = float(np.polyfit(np.arange(len(arr)), arr, 1)[0])
        else:
            trend_slope = None

        # Score: higher dependency = higher stress (less circular self-sufficiency)
        # Benchmark: dependency_index ~5 corresponds to moderate open economy
        benchmark = 5.0
        ratio = dependency_index / benchmark
        raw_score = float(np.clip(ratio * 40.0, 0.0, 100.0))
        score = float(np.clip(raw_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "trade_pct_gdp_latest": round(trade_latest, 2),
            "industrial_co2_share_pct": round(ind_co2_latest, 2) if ind_co2_latest is not None else None,
            "dependency_index": round(dependency_index, 4),
            "trade_trend_slope": round(trend_slope, 4) if trend_slope is not None else None,
            "benchmark_dependency_index": benchmark,
        }
