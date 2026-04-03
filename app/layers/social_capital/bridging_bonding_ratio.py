"""Bridging-Bonding Ratio module.

Estimates the ratio of outward-facing (bridging) to inward-facing (bonding)
social capital using:
  NE.TRD.GNFS.ZS  - Trade openness (% of GDP) as bridging proxy
  RL.EST           - Rule of Law as institutional trust enabling bridging

High trade openness + strong rule of law = healthy bridging capital = lower stress.

Score formula:
  trade_pct normalized to 0-100 scale (cap at 200% of GDP for full range)
  bridging_score  = clip(100 - min(trade_pct, 200) / 200 * 100, 0, 100)
                   [inverted: high openness = low stress]
  law_score       = clip(50 - rl_est * 20, 0, 100)
  score = mean of available component scores

Sources: World Bank WDI (NE.TRD.GNFS.ZS, RL.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class BridgingBondingRatio(LayerBase):
    layer_id = "lSC"
    name = "Bridging-Bonding Ratio"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('NE.TRD.GNFS.ZS', 'RL.EST')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for NE.TRD.GNFS.ZS or RL.EST",
            }

        latest: dict[str, float] = {}
        series_values: dict[str, list[float]] = {}
        for r in rows:
            series_values.setdefault(r["series_id"], []).append(float(r["value"]))
        for sid, vals in series_values.items():
            latest[sid] = vals[-1]

        component_scores: list[float] = []
        bridging_score = None
        law_score = None

        if "NE.TRD.GNFS.ZS" in latest:
            trade_pct = latest["NE.TRD.GNFS.ZS"]
            # High openness = strong bridging = low stress; cap normalization at 200% of GDP
            bridging_score = float(np.clip(100.0 - min(trade_pct, 200.0) / 200.0 * 100.0, 0.0, 100.0))
            component_scores.append(bridging_score)

        if "RL.EST" in latest:
            rl = latest["RL.EST"]
            law_score = float(np.clip(50.0 - rl * 20.0, 0.0, 100.0))
            component_scores.append(law_score)

        if not component_scores:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        score = float(np.mean(component_scores))

        return {
            "score": round(score, 1),
            "country": country,
            "trade_openness_pct_gdp": round(latest.get("NE.TRD.GNFS.ZS", float("nan")), 2),
            "rule_of_law_est": round(latest.get("RL.EST", float("nan")), 4),
            "bridging_stress_score": round(bridging_score, 1) if bridging_score is not None else None,
            "law_stress_score": round(law_score, 1) if law_score is not None else None,
            "n_components": len(component_scores),
            "note": "High score = weak bridging capital relative to bonding",
        }
