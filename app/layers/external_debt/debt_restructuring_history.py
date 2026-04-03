"""Debt Restructuring History module.

Assesses a country's history of external debt restructuring events (Paris Club
agreements, HIPC completion points, private sector restructurings) as a proxy
for chronic debt vulnerability. Countries with repeated restructuring episodes
face persistent market access constraints and elevated spreads.

Methodology:
- Query the analysis_results table for any stored restructuring episode metadata
  tagged with layer_id = 'lXD' and module = 'debt_restructuring_history'.
- Fallback: query data_series for IC.LGL.DURS.DB (contract enforcement) as a
  governance/institutional proxy when direct restructuring data is absent.
- Restructuring count from stored results drives the score.
- Score = clip(episode_count / 5 * 100, 0, 100): 5+ episodes = max stress.
- If no data, returns UNAVAILABLE.

Sources: Internal analysis_results store; World Bank WDI (IC.LGL.DURS.DB fallback)
"""

from __future__ import annotations

import json

import numpy as np

from app.layers.base import LayerBase


class DebtRestructuringHistory(LayerBase):
    layer_id = "lXD"
    name = "Debt Restructuring History"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # Attempt to read stored restructuring episode count from analysis_results
        stored_rows = await db.fetch_all(
            """
            SELECT result_json, created_at
            FROM analysis_results
            WHERE country_iso3 = ?
              AND layer_id = 'lXD'
              AND module = 'debt_restructuring_history'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (country,),
        )

        if stored_rows:
            try:
                result_data = json.loads(stored_rows[0]["result_json"])
                episode_count = int(result_data.get("episode_count", 0))
                score = float(np.clip(episode_count / 5.0 * 100, 0, 100))
                return {
                    "score": round(score, 1),
                    "country": country,
                    "episode_count": episode_count,
                    "episodes": result_data.get("episodes", []),
                    "source": "analysis_results",
                    "as_of": stored_rows[0]["created_at"],
                }
            except (json.JSONDecodeError, KeyError, ValueError):
                pass

        # Fallback: use contract enforcement duration as institutional proxy
        proxy_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IC.LGL.DURS.DB'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not proxy_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no restructuring history or proxy data available",
            }

        latest = next((r for r in proxy_rows if r["value"] is not None), None)
        if latest is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all proxy values null"}

        # Longer enforcement duration = weaker institutions = higher proxy risk
        enforcement_days = float(latest["value"])
        # Global range roughly 100-2000 days; normalise to stress score
        proxy_score = float(np.clip(enforcement_days / 2000.0 * 100, 0, 100))

        return {
            "score": round(proxy_score, 1),
            "country": country,
            "proxy_used": "contract_enforcement_duration_days",
            "enforcement_days": round(enforcement_days, 1),
            "reference_date": latest["date"],
            "note": "direct restructuring episode data absent; institutional proxy used",
            "indicators": ["IC.LGL.DURS.DB"],
        }
