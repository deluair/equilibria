"""Global Risk Appetite module.

Capital flow volatility as proxy for global risk appetite shock exposure.
Queries WDI:
  - BX.KLT.DINV.WD.GD.ZS : FDI inflows, net, % of GDP (primary)
  - BX.PEF.TOTL.CD.WD     : Portfolio equity inflows, current USD (secondary, if available)

Methodology:
  - High volatility in FDI inflows signals exposure to global risk-off episodes.
  - Countries with highly volatile inflows face stop-and-go capital dynamics.
  - Coefficient of variation (CV = std/mean) captures relative volatility.

Score = clip(cv_fdi * 30 + fdi_vol_norm * 20, 0, 100)
  where fdi_vol_norm = clip(std_fdi / 2, 0, 1) normalizes absolute volatility.

Sources: World Bank WDI, Rey (2015) global financial cycle, Miranda-Agrippino & Rey (2020).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GlobalRiskAppetite(LayerBase):
    layer_id = "lRI"
    name = "Global Risk Appetite"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        async def fetch_series(series_id: str) -> list[float]:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY dp.date
                """,
                (country, series_id),
            )
            return [float(r["value"]) for r in rows]

        fdi_vals = await fetch_series("BX.KLT.DINV.WD.GD.ZS")
        portfolio_vals = await fetch_series("BX.PEF.TOTL.CD.WD")

        # Need at least FDI data
        if len(fdi_vals) < 8:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient FDI data (need >= 8 obs)",
            }

        fdi = np.array(fdi_vals)
        fdi_mean = float(np.mean(fdi))
        fdi_std = float(np.std(fdi, ddof=1))

        # Coefficient of variation (use absolute mean to avoid sign issues)
        abs_mean = abs(fdi_mean) if abs(fdi_mean) > 1e-6 else 1e-6
        cv_fdi = fdi_std / abs_mean

        # Normalized absolute volatility (std > 2pp = high)
        fdi_vol_norm = float(np.clip(fdi_std / 2.0, 0, 1))

        score = float(np.clip(cv_fdi * 30 + fdi_vol_norm * 20, 0, 100))

        # Portfolio equity supplementary info
        portfolio_info = None
        if len(portfolio_vals) >= 5:
            pf = np.array(portfolio_vals)
            portfolio_info = {
                "mean_usd": round(float(np.mean(pf)), 0),
                "std_usd": round(float(np.std(pf, ddof=1)), 0),
                "cv": round(float(np.std(pf, ddof=1) / max(abs(float(np.mean(pf))), 1e6)), 4),
                "n_obs": len(portfolio_vals),
            }

        flags = []
        if cv_fdi > 2:
            flags.append(f"very high FDI coefficient of variation: {cv_fdi:.2f}")
        if fdi_std > 3:
            flags.append(f"high FDI volatility: {fdi_std:.2f}pp std dev")
        if fdi_mean < 0:
            flags.append("net FDI outflows on average (disinvestment)")

        return {
            "score": round(score, 1),
            "country": country,
            "fdi_mean_pct_gdp": round(fdi_mean, 4),
            "fdi_std_pct_gdp": round(fdi_std, 4),
            "fdi_cv": round(cv_fdi, 4),
            "n_fdi_obs": len(fdi_vals),
            "portfolio_equity": portfolio_info,
            "flags": flags,
            "reference": "Rey 2015 global financial cycle; Miranda-Agrippino & Rey 2020",
        }
