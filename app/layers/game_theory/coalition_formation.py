"""Coalition Formation module.

Assesses trade bloc/coalition effectiveness via persistent trade imbalance
(Riker 1962, Baldwin 2006 on trade coalitions).

In an effective coalition, members gain balanced trade growth. Persistent
divergence between export and import growth signals coalition failure or
one-sided dependence rather than reciprocal gains from trade.

Score = magnitude of sustained export-import growth gap, clipped [0, 100].

Sources: WDI (NE.EXP.GNFS.KD.ZG, NE.IMP.GNFS.KD.ZG)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CoalitionFormation(LayerBase):
    layer_id = "lGT"
    name = "Coalition Formation"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.EXP.GNFS.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        imp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.IMP.GNFS.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not exp_rows or not imp_rows or len(exp_rows) < 5 or len(imp_rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data: need NE.EXP.GNFS.KD.ZG and NE.IMP.GNFS.KD.ZG (min 5 obs)",
            }

        exp_map = {r["date"]: float(r["value"]) for r in exp_rows}
        imp_map = {r["date"]: float(r["value"]) for r in imp_rows}
        common = sorted(set(exp_map) & set(imp_map))

        if len(common) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "fewer than 5 overlapping dates",
            }

        exp_arr = np.array([exp_map[d] for d in common])
        imp_arr = np.array([imp_map[d] for d in common])

        exp_mean = float(np.mean(exp_arr))
        imp_mean = float(np.mean(imp_arr))

        # Persistent imbalance in growth rates
        gap_series = exp_arr - imp_arr
        mean_gap = float(np.mean(gap_series))
        gap_std = float(np.std(gap_series))

        # t-stat analog: how consistently is gap non-zero?
        t_stat = abs(mean_gap) / max(gap_std / np.sqrt(len(gap_series)), 1e-10)

        # Score: large persistent gap = coalition failure
        # abs gap scaled: |gap| > 10 pp = high stress; t-stat amplifies
        base_score = float(np.clip(abs(mean_gap) / 10.0 * 60.0, 0.0, 60.0))
        persistence_bonus = float(np.clip((t_stat - 1.0) * 10.0, 0.0, 40.0))
        score = float(np.clip(base_score + persistence_bonus, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "export_growth_mean_pct": round(exp_mean, 3),
            "import_growth_mean_pct": round(imp_mean, 3),
            "mean_growth_gap_pp": round(mean_gap, 3),
            "gap_std": round(gap_std, 4),
            "gap_t_stat": round(t_stat, 4),
            "n_obs": len(common),
            "period": f"{common[0]} to {common[-1]}",
            "interpretation": (
                "coalition failure: persistent trade growth imbalance" if score > 60
                else "moderate trade coalition stress" if score > 30
                else "balanced trade coalition dynamics"
            ),
        }
