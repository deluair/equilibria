"""Terms of Trade (Commodity) module.

Measures commodity export/import price ratio dynamics. A deteriorating
commodity terms of trade signals declining purchasing power of exports
and rising import costs, increasing current account pressure.

Methodology:
- Query commodity export unit value index (TT.PRI.MRCH.XD.WD or equivalent).
- Query commodity import unit value index.
- ToT = export_price_index / import_price_index * 100.
- Trend: OLS slope over last 10 years (annualized).
- Score: baseline 30; penalize if ToT < 100 (deterioration) or falling trend.

Sources: World Bank WDI (TT.PRI.MRCH.XD.WD, TT.PRI.MRCH.MD.WD).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TermsOfTradeCommodity(LayerBase):
    layer_id = "lCM"
    name = "Terms of Trade (Commodity)"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def _series(series_id: str) -> dict[str, float]:
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date
                """,
                (country, series_id),
            )
            return {row["date"]: float(row["value"]) for row in rows}

        export_px = await _series("TT.PRI.MRCH.XD.WD")
        import_px = await _series("TT.PRI.MRCH.MD.WD")

        if not export_px or not import_px:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no terms of trade data"}

        common = sorted(set(export_px) & set(import_px))
        if not common:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping dates"}

        tot_series = [export_px[d] / import_px[d] * 100 for d in common if import_px[d] > 0]
        if not tot_series:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero import prices"}

        latest_tot = tot_series[-1]
        t_arr = np.arange(len(tot_series), dtype=float)
        slope = 0.0
        if len(t_arr) >= 5:
            slope, _, _, _, _ = np.polyfit(t_arr, tot_series, 1, full=False), *([None] * 4)  # type: ignore[misc]
            coeffs = np.polyfit(t_arr, tot_series, 1)
            slope = float(coeffs[0])

        score = 20.0
        if latest_tot < 100:
            score += min((100 - latest_tot) * 0.5, 40)
        if slope < 0:
            score += min(abs(slope) * 2, 30)
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "latest_tot": round(latest_tot, 2),
            "latest_date": common[-1],
            "trend_slope": round(slope, 3),
            "deteriorating": latest_tot < 100,
            "n_obs": len(tot_series),
            "indicators": ["TT.PRI.MRCH.XD.WD", "TT.PRI.MRCH.MD.WD"],
        }
