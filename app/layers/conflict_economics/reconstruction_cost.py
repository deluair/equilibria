"""Reconstruction Cost module.

Estimates post-conflict reconstruction needs as a function of capital stock
destruction implied by cumulative GDP losses below trend. Uses GDP per capita
(NY.GDP.PCAP.KD) and investment rates (NE.GDI.TOTL.ZS) to estimate
reconstruction burden relative to economic size.

Score = clip(reconstruction_burden_index * 100, 0, 100).
High score = heavy reconstruction cost relative to GDP.

Sources: WDI (NY.GDP.PCAP.KD, NE.GDI.TOTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ReconstructionCost(LayerBase):
    layer_id = "lCW"
    name = "Reconstruction Cost"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        gdppc_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date
            LIMIT 30
            """,
            (country,),
        )

        inv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.TOTL.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not gdppc_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        gdppc_vals = [float(r["value"]) for r in gdppc_rows if r["value"] is not None]
        if len(gdppc_vals) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        arr = np.array(gdppc_vals)

        # Trend via linear fit
        t = np.arange(len(arr))
        coeffs = np.polyfit(t, arr, 1)
        trend = np.polyval(coeffs, t)

        # Cumulative gap below trend (proxy for capital destruction)
        gap = trend - arr
        cumulative_loss = float(np.sum(np.maximum(gap, 0)))
        avg_gdppc = float(np.mean(arr))
        reconstruction_ratio = cumulative_loss / (avg_gdppc * len(arr)) if avg_gdppc > 0 else 0.0

        # Investment rate: low investment -> harder reconstruction
        inv_vals = [float(r["value"]) for r in inv_rows if r["value"] is not None]
        inv_mean = float(np.mean(inv_vals)) if inv_vals else None

        # Low investment amplifies reconstruction burden
        inv_penalty = float(np.clip((25 - inv_mean) / 25 * 20, 0, 20)) if inv_mean is not None else 0.0

        base_score = float(np.clip(reconstruction_ratio * 200, 0, 80))
        score = float(np.clip(base_score + inv_penalty, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gdppc_mean": round(avg_gdppc, 2),
            "cumulative_gap_below_trend": round(cumulative_loss, 2),
            "reconstruction_ratio": round(reconstruction_ratio, 6),
            "investment_rate_mean": round(inv_mean, 4) if inv_mean is not None else None,
            "inv_penalty": round(inv_penalty, 2),
            "n_obs": len(gdppc_vals),
            "indicators": {
                "gdp_per_capita": "NY.GDP.PCAP.KD",
                "investment_rate": "NE.GDI.TOTL.ZS",
            },
        }
