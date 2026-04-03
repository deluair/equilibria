"""Water resource stress: freshwater availability per capita vs demand.

Uses World Bank WDI:
  ER.H2O.INTR.PC   - renewable internal freshwater resources per capita (cubic meters)
  ER.H2O.FWTL.ZS   - annual freshwater withdrawals (% of internal resources)

Water stress is measured via the Falkenmark water scarcity index (per-capita
renewable resources) and the withdrawal-to-availability ratio (WTA).

Score:
  s_scarcity = based on Falkenmark thresholds (1700/1000/500 m3/capita/year)
  s_withdrawal = clip(withdrawal_pct * 0.5, 0, 50)
  score = clip(s_scarcity + s_withdrawal, 0, 100)

Sources: World Bank WDI (ER.H2O.INTR.PC, ER.H2O.FWTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WaterResourceStress(LayerBase):
    layer_id = "lNR"
    name = "Water Resource Stress"
    weight = 0.20

    # Falkenmark water scarcity thresholds (m3/capita/year)
    FALKENMARK_ABSOLUTE = 500.0    # absolute scarcity
    FALKENMARK_SCARCITY = 1000.0   # water scarcity
    FALKENMARK_STRESS = 1700.0     # water stress

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN ('ER.H2O.INTR.PC', 'ER.H2O.FWTL.ZS')
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 30
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no freshwater resource data",
            }

        latest: dict[str, tuple[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest and r["value"] is not None:
                latest[sid] = (r["date"][:4], float(r["value"]))

        pc_data = latest.get("ER.H2O.INTR.PC")
        wta_data = latest.get("ER.H2O.FWTL.ZS")

        if pc_data is None and wta_data is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no freshwater availability or withdrawal data",
            }

        # Falkenmark scarcity sub-score (0-50)
        s_scarcity = 0.0
        fw_pc_m3 = None
        scarcity_category = "data_unavailable"
        if pc_data:
            fw_pc_m3 = pc_data[1]
            if fw_pc_m3 < self.FALKENMARK_ABSOLUTE:
                s_scarcity = 50.0
                scarcity_category = "absolute_scarcity"
            elif fw_pc_m3 < self.FALKENMARK_SCARCITY:
                s_scarcity = 40.0
                scarcity_category = "water_scarcity"
            elif fw_pc_m3 < self.FALKENMARK_STRESS:
                s_scarcity = 25.0
                scarcity_category = "water_stress"
            else:
                s_scarcity = float(np.clip(1700.0 / fw_pc_m3 * 10.0, 0, 15))
                scarcity_category = "adequate"

        # Withdrawal-to-availability sub-score (0-50)
        s_withdrawal = 0.0
        withdrawal_pct = None
        if wta_data:
            withdrawal_pct = wta_data[1]
            s_withdrawal = float(np.clip(withdrawal_pct * 0.5, 0, 50))

        score = float(np.clip(s_scarcity + s_withdrawal, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "freshwater_per_capita_m3": (
                    round(fw_pc_m3, 1) if fw_pc_m3 is not None else None
                ),
                "withdrawal_pct_internal_resources": (
                    round(withdrawal_pct, 2) if withdrawal_pct is not None else None
                ),
                "scarcity_category": scarcity_category,
                "falkenmark_thresholds_m3": {
                    "absolute_scarcity": self.FALKENMARK_ABSOLUTE,
                    "water_scarcity": self.FALKENMARK_SCARCITY,
                    "water_stress": self.FALKENMARK_STRESS,
                },
                "sub_scores": {
                    "falkenmark_scarcity": round(s_scarcity, 2),
                    "withdrawal_pressure": round(s_withdrawal, 2),
                },
            },
        }
