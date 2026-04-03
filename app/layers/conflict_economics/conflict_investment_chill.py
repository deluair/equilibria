"""Conflict Investment Chill module.

Measures FDI suppression attributable to conflict risk. Uses net FDI inflows
(BX.KLT.DINV.WD.GD.ZS as % of GDP) relative to regional/income-group norms.
Political stability (PV.EST) is used to identify whether FDI suppression is
conflict-driven versus structural.

Score = clip(chill_index * 100, 0, 100).
High score = severe FDI suppression from conflict risk.

Sources: WDI (BX.KLT.DINV.WD.GD.ZS, PV.EST)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ConflictInvestmentChill(LayerBase):
    layer_id = "lCW"
    name = "Conflict Investment Chill"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        fdi_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.KLT.DINV.WD.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        stability_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PV.EST'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not fdi_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        fdi_vals = [float(r["value"]) for r in fdi_rows if r["value"] is not None]
        if not fdi_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        fdi_mean = float(np.mean(fdi_vals))
        fdi_std = float(np.std(fdi_vals))

        # Global FDI benchmark: ~2.5% of GDP is average developing country
        benchmark = 2.5
        fdi_gap = max(benchmark - fdi_mean, 0.0)

        # Volatility of FDI (uncertainty-driven deterrence)
        fdi_cv = fdi_std / abs(fdi_mean) if abs(fdi_mean) > 1e-6 else fdi_std

        # Political stability amplifier (low stability -> conflict-attributed FDI loss)
        stability_vals = [float(r["value"]) for r in stability_rows if r["value"] is not None]
        stability_mean = float(np.mean(stability_vals)) if stability_vals else None

        # Normalize stability from [-2.5, 2.5] to [0, 1]; low = more conflict attribution
        if stability_mean is not None:
            stability_norm = (stability_mean + 2.5) / 5.0
            conflict_attribution = 1.0 - stability_norm
        else:
            conflict_attribution = 0.5

        gap_component = float(np.clip(fdi_gap * 10, 0, 50))
        vol_component = float(np.clip(fdi_cv * 15, 0, 30))
        attribution_weight = float(np.clip(conflict_attribution * 20, 0, 20))

        score = float(np.clip((gap_component + vol_component) * (0.5 + conflict_attribution * 0.5) + attribution_weight, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "fdi_pct_gdp_mean": round(fdi_mean, 4),
            "fdi_pct_gdp_std": round(fdi_std, 4),
            "fdi_gap_from_benchmark": round(fdi_gap, 4),
            "fdi_cv": round(fdi_cv, 4),
            "political_stability_est": round(stability_mean, 4) if stability_mean is not None else None,
            "conflict_attribution": round(conflict_attribution, 4),
            "gap_component": round(gap_component, 2),
            "vol_component": round(vol_component, 2),
            "attribution_weight": round(attribution_weight, 2),
            "n_obs": len(fdi_vals),
            "indicators": {
                "fdi_net_inflows": "BX.KLT.DINV.WD.GD.ZS",
                "political_stability": "PV.EST",
            },
        }
