"""Ally Trade Dependence: trade openness x governance alignment proxy.

Measures how much a country's trade is structured around governance-aligned
partners. High trade openness (NE.TRD.GNFS.ZS) combined with strong rule of
law (RL.EST) suggests trade with aligned, rules-based partners -- lower risk.
Low rule of law with high trade openness = dependence on non-aligned partners.

Methodology:
    trade_open = latest NE.TRD.GNFS.ZS
    rl_raw = latest RL.EST (rule of law WGI, -2.5 to 2.5)
    rl_norm = (rl_raw + 2.5) / 5.0 clamped [0, 1]  -- 1 = strong rule of law
    alignment_gap = (1 - rl_norm) * trade_open / 100.0
    score = clip(alignment_gap * 100, 0, 100)

    High score = large trade volume with weak-governance partners.

Score (0-100): Higher = greater non-aligned ally trade dependence risk.

References:
    Aiyar et al. (2023). "Geoeconomic Fragmentation and the Future of
        Multilateralism." IMF SDN/2023/001.
    Gopinath, G. (2022). "How Will the Pandemic and War Shape Future
        IMF Surveillance?" IMF Blog.
    World Bank WDI NE.TRD.GNFS.ZS, RL.EST.
"""

from __future__ import annotations

from app.layers.base import LayerBase

_TRADE_CODE = "NE.TRD.GNFS.ZS"
_RL_CODE = "RL.EST"


class AllyTradeDependence(LayerBase):
    layer_id = "lGP"
    name = "Ally Trade Dependence"

    async def _fetch_latest(self, db, code: str, name: str) -> float | None:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        for r in rows:
            if r["value"] is not None:
                return float(r["value"])
        return None

    async def compute(self, db, **kwargs) -> dict:
        trade_open = await self._fetch_latest(db, _TRADE_CODE, "trade % gdp")
        rl_raw = await self._fetch_latest(db, _RL_CODE, "rule of law")

        if trade_open is None and rl_raw is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for NE.TRD.GNFS.ZS or RL.EST"}

        rl_norm = max(0.0, min(1.0, ((rl_raw or 0.0) + 2.5) / 5.0))
        alignment_gap = (1.0 - rl_norm) * (trade_open or 0.0) / 100.0
        score = float(min(max(alignment_gap * 100, 0.0), 100.0))

        return {
            "score": round(score, 2),
            "trade_openness_pct_gdp": round(trade_open, 2) if trade_open is not None else None,
            "rule_of_law_est": round(rl_raw, 4) if rl_raw is not None else None,
            "governance_alignment_norm": round(rl_norm, 4),
            "alignment_gap": round(alignment_gap, 4),
            "metrics": {
                "trade_indicator": _TRADE_CODE,
                "governance_indicator": _RL_CODE,
            },
        }
