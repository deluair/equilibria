"""Interconnectedness Risk.

Financial sector interconnectedness proxy: high trade openness combined with
high FDI flow volatility indicates cross-border exposure amplification.

Score (0-100): clip(openness_pct/100 * volatility_coef * 100, 0, 100).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InterconnectednessRisk(LayerBase):
    layer_id = "lMP"
    name = "Interconnectedness Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 15)

        rows = await db.fetch_all(
            """
            SELECT ds.series_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_code IN ('NE.TRD.GNFS.ZS', 'BX.KLT.DINV.WD.GD.ZS')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.series_code, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no trade openness or FDI data",
            }

        trade_vals: list[float] = []
        fdi_vals: list[float] = []

        for r in rows:
            if r["series_code"] == "NE.TRD.GNFS.ZS":
                trade_vals.append(float(r["value"]))
            elif r["series_code"] == "BX.KLT.DINV.WD.GD.ZS":
                fdi_vals.append(float(r["value"]))

        openness = trade_vals[-1] if trade_vals else None
        fdi_volatility = float(np.std(fdi_vals)) if len(fdi_vals) >= 3 else None

        if openness is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "trade openness data missing",
            }

        fdi_vol_normalized = min(fdi_volatility / 5.0, 2.0) if fdi_volatility is not None else 1.0
        raw_score = (openness / 100.0) * fdi_vol_normalized * 100.0
        score = float(np.clip(raw_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "trade_openness_pct_gdp": round(openness, 2),
            "fdi_volatility_pct_gdp": round(fdi_volatility, 4) if fdi_volatility is not None else None,
            "fdi_observations": len(fdi_vals),
            "trade_observations": len(trade_vals),
            "interconnectedness_channel": "high openness x high capital flow volatility",
            "interpretation": self._interpret(openness, fdi_volatility),
        }

    @staticmethod
    def _interpret(openness: float, fdi_vol: float | None) -> str:
        vol_desc = f"FDI volatility {fdi_vol:.2f}%GDP" if fdi_vol is not None else "FDI volatility unknown"
        if openness > 100 and fdi_vol is not None and fdi_vol > 3:
            return f"high interconnectedness: openness {openness:.0f}% GDP, {vol_desc}"
        if openness > 60:
            return f"moderate interconnectedness: openness {openness:.0f}% GDP, {vol_desc}"
        return f"low interconnectedness: openness {openness:.0f}% GDP, {vol_desc}"
