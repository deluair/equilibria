"""Language Economics module.

Export partner diversity as a linguistic diversity proxy.

Herfindahl-Hirschman Index (HHI) of export concentration across partner
countries or product groups is used as a proxy for linguistic/cultural
openness. A highly concentrated export structure (reliant on one or two
partners) signals limited cross-cultural economic engagement.

Primary query: trade series with values aggregated by year.
HHI is computed from the distribution of annual trade values across
distinct series identifiers (partner/product proxies).

HHI range: 1/N (maximum diversity) to 1.0 (monopoly).
score = clip(HHI * 100, 0, 100)
- HHI = 1.0 (single partner) -> score = 100 (high stress)
- HHI = 0.1                   -> score = 10

Fallback: if < 5 distinct series, use variance of trade values as
a secondary concentration measure.

Sources: WDI (trade series), Comtrade (if available)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LanguageEconomics(LayerBase):
    layer_id = "lCU"
    name = "Language Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Attempt to get trade series with multiple distinct series_ids as partner proxies
        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND (ds.series_id LIKE '%TX%' OR ds.series_id LIKE '%BX%'
                   OR ds.series_id LIKE '%TM%' OR ds.series_id LIKE 'trade%')
              AND dp.value > 0
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data (need >= 5 rows)",
            }

        # Aggregate latest value per series as share proxy
        latest: dict[str, float] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest:
                latest[sid] = float(r["value"])

        values = np.array(list(latest.values()), dtype=float)
        values = values[values > 0]

        if len(values) < 2:
            # Fallback: variance-based concentration from full time series
            all_values = np.array([float(r["value"]) for r in rows if float(r["value"]) > 0])
            if len(all_values) < 5:
                return {
                    "score": None,
                    "signal": "UNAVAILABLE",
                    "error": "insufficient positive trade values",
                }
            cv = float(np.std(all_values) / np.mean(all_values)) if np.mean(all_values) > 0 else 1.0
            score = float(np.clip(cv * 50.0, 0.0, 100.0))
            return {
                "score": round(score, 1),
                "country": country,
                "n_obs": len(rows),
                "method": "cv_fallback",
                "coefficient_of_variation": round(cv, 4),
                "note": "fallback: CV of trade values; score = clip(CV*50, 0, 100)",
            }

        # Herfindahl-Hirschman Index
        shares = values / values.sum()
        hhi = float(np.sum(shares ** 2))
        n = len(shares)
        # Normalize HHI to [0, 1]: (HHI - 1/n) / (1 - 1/n)
        hhi_norm = (hhi - 1.0 / n) / (1.0 - 1.0 / n) if n > 1 else 1.0
        hhi_norm = float(np.clip(hhi_norm, 0.0, 1.0))

        score = float(np.clip(hhi_norm * 100.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(rows),
            "n_series": n,
            "hhi_raw": round(hhi, 4),
            "hhi_normalized": round(hhi_norm, 4),
            "method": "hhi",
            "note": "HHI of trade series value shares; high HHI = high concentration = high stress",
        }
