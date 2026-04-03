"""Economic Losses from Disasters module.

Measures GDP growth deviation in disaster-prone years. Correlates growth dips
(NY.GDP.MKTP.KD.ZG) with disaster exposure years derived from EN.CLC.MDAT.ZS.
High co-movement of growth dips with disaster exposure signals economic loss
transmission from natural hazards.

Score = clip(loss_transmission_index * 100, 0, 100)
  loss_transmission_index = correlation of growth shortfall with disaster years
  (when both series exist and overlap by >= 5 years)

Sources: WDI (NY.GDP.MKTP.KD.ZG, EN.CLC.MDAT.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class EconomicLossesFromDisasters(LayerBase):
    layer_id = "lDE"
    name = "Economic Losses from Disasters"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        growth_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        disaster_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.CLC.MDAT.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not growth_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        # Build date-indexed dicts
        growth_by_date = {
            r["date"]: float(r["value"])
            for r in growth_rows
            if r["value"] is not None
        }
        disaster_by_date = {
            r["date"]: float(r["value"])
            for r in disaster_rows
            if r["value"] is not None
        }

        # GDP growth volatility as primary signal
        growth_vals = np.array(list(growth_by_date.values()))
        growth_std = float(np.std(growth_vals)) if len(growth_vals) >= 3 else 0.0
        growth_mean = float(np.mean(growth_vals))

        # Overlapping dates for correlation
        common_dates = sorted(set(growth_by_date.keys()) & set(disaster_by_date.keys()))
        correlation = None
        if len(common_dates) >= 5:
            g = np.array([growth_by_date[d] for d in common_dates])
            d = np.array([disaster_by_date[d] for d in common_dates])
            # Shortfall: below-median growth years vs disaster exposure
            median_g = float(np.median(g))
            shortfall = np.maximum(0.0, median_g - g)
            if np.std(shortfall) > 1e-10 and np.std(d) > 1e-10:
                corr = float(np.corrcoef(shortfall, d)[0, 1])
                correlation = round(corr, 4)

        # Score: higher if growth volatile AND correlated with disaster exposure
        vol_component = float(np.clip(growth_std * 5, 0, 60))
        if correlation is not None and correlation > 0:
            corr_component = float(np.clip(correlation * 40, 0, 40))
        else:
            corr_component = 0.0
        score = float(np.clip(vol_component + corr_component, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "gdp_growth_mean": round(growth_mean, 4),
            "gdp_growth_std": round(growth_std, 4),
            "disaster_growth_correlation": correlation,
            "n_growth_obs": len(growth_vals),
            "n_overlap_obs": len(common_dates),
            "vol_component": round(vol_component, 2),
            "corr_component": round(corr_component, 2),
            "indicators": {
                "gdp_growth": "NY.GDP.MKTP.KD.ZG",
                "disaster_exposure": "EN.CLC.MDAT.ZS",
            },
        }
