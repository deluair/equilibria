"""Decommodification Index module.

Measures the degree to which welfare provision removes citizens from
pure market dependency. Operationalised as social transfers per unit of
per-capita income: higher spending relative to income = more
decommodified labour force.

Score = clip(100 - (transfers_pct / max(gdp_pcap_ratio, 0.001)) * 10, 0, 100)
where gdp_pcap_ratio normalises per-capita GDP to a 0-1 scale using a
$50,000 ceiling (approximate high-income benchmark).

Higher score = less decommodification (more market dependency).

Sources: WDI GC.XPN.TRFT.ZS (social transfers % expense),
         WDI NY.GDP.PCAP.KD (GDP per capita, constant 2015 USD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

GDP_PCAP_CEILING = 50_000.0  # USD constant 2015, high-income benchmark


class DecommodificationIndex(LayerBase):
    layer_id = "lWS"
    name = "Decommodification Index"

    async def _fetch_mean(self, db, code: str, name: str) -> float | None:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        return float(np.mean(vals)) if vals else None

    async def compute(self, db, **kwargs) -> dict:
        transfers = await self._fetch_mean(db, "GC.XPN.TRFT.ZS", "social transfers")
        gdp_pcap = await self._fetch_mean(db, "NY.GDP.PCAP.KD", "GDP per capita")

        if transfers is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no social transfers data"}

        trns = transfers
        # Normalise GDP per capita to 0-1 scale; default to middle-income level if missing
        gdp_ratio = min(1.0, gdp_pcap / GDP_PCAP_CEILING) if gdp_pcap is not None else 0.3

        # Decommodification proxy: transfers relative to income level
        # Higher gdp_ratio means citizens need more coverage to be decommodified
        decommodification = trns * gdp_ratio  # scaled generosity
        # Score: 100 when no decommodification, 0 when fully decommodified
        score = float(np.clip(100.0 - min(100.0, decommodification * 5.0), 0, 100))

        return {
            "score": round(score, 1),
            "social_transfers_pct": round(trns, 2),
            "gdp_per_capita_usd": round(gdp_pcap, 0) if gdp_pcap is not None else None,
            "gdp_normalised_ratio": round(gdp_ratio, 3),
            "decommodification_proxy": round(decommodification, 3),
            "interpretation": (
                "highly commodified (market-dependent)" if score > 75
                else "limited decommodification" if score > 50
                else "partial decommodification" if score > 25
                else "strong decommodification"
            ),
            "sources": ["WDI GC.XPN.TRFT.ZS", "WDI NY.GDP.PCAP.KD"],
        }
