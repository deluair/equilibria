"""Geographic Disadvantage module.

Proxies landlocked or small-island economic disadvantage by measuring trade
openness relative to income level. Landlocked and remote economies should
exhibit lower trade openness at any income level; the residual from a simple
income-openness relationship captures this structural disadvantage.

Approach:
  1. Query trade openness (NE.TRD.GNFS.ZS) and GDP per capita (NY.GDP.PCAP.KD)
     for the country over available years.
  2. Compute predicted openness from a linear fit of log(income) on openness
     using available years as the cross-sectional signal.
  3. Residual = actual - predicted. Large negative residual = geographically
     disadvantaged (less open than income level predicts).

Score = clip(-residual_pct, 0, 100)
Positive score only when actual openness is below the predicted level.

Sources: WDI NE.TRD.GNFS.ZS, WDI NY.GDP.PCAP.KD
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GeographicDisadvantage(LayerBase):
    layer_id = "lRD"
    name = "Geographic Disadvantage"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_trade = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        rows_gdp = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date DESC
            LIMIT 15
            """,
            (country,),
        )

        if not rows_trade or not rows_gdp:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        trade_map = {r["date"]: float(r["value"]) for r in rows_trade if r["value"] is not None}
        gdp_map = {r["date"]: float(r["value"]) for r in rows_gdp if r["value"] is not None}

        common_dates = sorted(set(trade_map) & set(gdp_map), reverse=True)
        if len(common_dates) < 3:
            # Fallback: use latest trade openness directly -- low openness = high disadvantage
            trade_vals = [float(r["value"]) for r in rows_trade if r["value"] is not None]
            if not trade_vals:
                return {"score": None, "signal": "UNAVAILABLE", "error": "no valid values"}
            mean_trade = float(np.mean(trade_vals))
            score = float(np.clip(100 - mean_trade, 0, 100))
            return {
                "score": round(score, 1),
                "country": country,
                "method": "fallback_openness",
                "mean_trade_openness_pct": round(mean_trade, 2),
                "note": "residual method requires >= 3 matched year observations",
                "series": "NE.TRD.GNFS.ZS",
            }

        openness = np.array([trade_map[d] for d in common_dates])
        log_income = np.log(np.array([max(gdp_map[d], 1.0) for d in common_dates]))

        # Linear fit: openness ~ a + b * log_income
        coeffs = np.polyfit(log_income, openness, 1)
        predicted = np.polyval(coeffs, log_income)
        residuals = openness - predicted

        # Latest observation residual
        latest_residual = float(residuals[0])
        mean_residual = float(np.mean(residuals))

        # Negative residual = less open than income predicts = geographic disadvantage
        score = float(np.clip(-latest_residual, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "latest_date": common_dates[0],
            "latest_trade_openness_pct": round(openness[0], 2),
            "predicted_openness_pct": round(float(predicted[0]), 2),
            "residual_pct": round(latest_residual, 2),
            "mean_residual_pct": round(mean_residual, 2),
            "n_obs": len(common_dates),
            "method": "income_openness_residual",
            "series": {
                "trade": "NE.TRD.GNFS.ZS",
                "income": "NY.GDP.PCAP.KD",
            },
            "interpretation": "negative residual = structurally less open than income predicts (geographic disadvantage)",
        }
