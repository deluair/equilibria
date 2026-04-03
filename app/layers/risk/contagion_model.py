"""Contagion Model module.

Financial contagion vulnerability: trade openness x financial integration.
Queries WDI:
  - NE.TRD.GNFS.ZS      : Trade (exports + imports) as % of GDP
  - BX.KLT.DINV.WD.GD.ZS : FDI inflows as % of GDP

High trade openness combined with high financial integration creates
broader channels for contagion transmission from external shocks.

Score = clip(sqrt(trade_pct * fdi_pct) * 2, 0, 100)
  Uses geometric mean to capture joint exposure. A country with high trade
  but zero FDI (or vice versa) scores lower than one exposed on both channels.

Sources: World Bank WDI, Forbes & Rigobon (2002) contagion theory.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ContagionModel(LayerBase):
    layer_id = "lRI"
    name = "Contagion Model"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def fetch_mean(series_id: str, n: int = 5) -> float | None:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY dp.date DESC
                LIMIT ?
                """,
                (country, series_id, n),
            )
            if not rows:
                return None
            return float(np.mean([float(r["value"]) for r in rows]))

        trade_pct = await fetch_mean("NE.TRD.GNFS.ZS")
        fdi_pct = await fetch_mean("BX.KLT.DINV.WD.GD.ZS")

        if trade_pct is None and fdi_pct is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no trade openness or FDI data",
            }

        t = max(trade_pct if trade_pct is not None else 0.0, 0.0)
        f = max(fdi_pct if fdi_pct is not None else 0.0, 0.0)

        # Geometric mean captures joint channel exposure
        geo_mean = float(np.sqrt(t * f)) if (t > 0 and f > 0) else float(max(t, f) * 0.5)
        score = float(np.clip(geo_mean * 2.0, 0, 100))

        channels = []
        if t > 80:
            channels.append(f"high trade openness: {t:.1f}% GDP")
        if f > 5:
            channels.append(f"high FDI integration: {f:.1f}% GDP")

        return {
            "score": round(score, 1),
            "country": country,
            "trade_pct_gdp": round(t, 2) if trade_pct is not None else None,
            "fdi_inflows_pct_gdp": round(f, 2) if fdi_pct is not None else None,
            "contagion_channels": channels,
            "method": "geometric mean of trade openness and FDI integration",
            "reference": "Forbes & Rigobon 2002 contagion identification",
        }
