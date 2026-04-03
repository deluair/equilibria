"""GVC Environmental Footprint module.

Estimates carbon intensity of a country's GVC position via CO2 emissions
per unit of export value (emissions embodied in trade proxy).

High ratio signals that the country occupies carbon-intensive segments of
GVCs (heavy industry, energy-intensive manufacturing, resource extraction).
Low ratio suggests clean-tech, services, or high-value manufacturing.

Calculation:
  ratio = CO2_kt / exports_constant_USD (billions)
  CO2_intensity = CO2_kt per billion USD of exports

Score = clip(ratio / threshold * 50, 0, 100).
  threshold = 500 kt CO2 per bn USD (rough global average for manufacturing).
  ratio >= 1000: score near 100 (very carbon-intensive GVC position).
  ratio ~0:      score near 0 (clean GVC position).

Sources: World Bank WDI (EN.ATM.CO2E.KT, NE.EXP.GNFS.KD).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_THRESHOLD_KT_PER_BN_USD = 500.0  # reference point for scoring


class GVCEnvironmentalFootprint(LayerBase):
    layer_id = "lVC"
    name = "GVC Environmental Footprint"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        co2_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.ATM.CO2E.KT'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        exp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.EXP.GNFS.KD'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not co2_rows or not exp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient CO2 or export data"}

        co2_vals = np.array([float(r["value"]) for r in co2_rows])
        exp_vals = np.array([float(r["value"]) for r in exp_rows])

        min_len = min(len(co2_vals), len(exp_vals))
        if min_len < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient aligned data"}

        co2_vals = co2_vals[:min_len]
        exp_vals = exp_vals[:min_len]

        # Exports are in constant LCU; convert to relative index
        # (ratio units cancel in relative comparisons)
        exp_safe = np.where(exp_vals < 1.0, 1.0, exp_vals)
        # Express exports in billions of constant units for interpretability
        exp_bn = exp_safe / 1e9

        ratios = co2_vals / exp_bn  # kt CO2 per billion USD (constant)
        mean_ratio = float(np.mean(ratios))
        latest_ratio = float(ratios[0])

        score = float(np.clip(mean_ratio / _THRESHOLD_KT_PER_BN_USD * 50.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "mean_co2_intensity_kt_per_bn_usd": round(mean_ratio, 2),
            "latest_co2_intensity": round(latest_ratio, 2),
            "mean_co2_kt": round(float(np.mean(co2_vals)), 0),
            "n_obs": min_len,
            "threshold_kt_per_bn_usd": _THRESHOLD_KT_PER_BN_USD,
            "interpretation": (
                "carbon-intensive GVC position" if mean_ratio > 1000
                else "moderate GVC carbon footprint" if mean_ratio > 500
                else "low-carbon GVC position"
            ),
        }
