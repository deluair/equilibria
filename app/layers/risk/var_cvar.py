"""Value at Risk and Conditional Value at Risk module.

Historical VaR and CVaR on GDP growth distribution.
Queries WDI: NY.GDP.MKTP.KD.ZG (GDP growth, annual %).

Methodology:
  - Historical simulation (non-parametric).
  - VaR(5%) = 5th percentile of empirical growth distribution.
  - CVaR(5%) = mean of all observations <= VaR(5%) (Expected Shortfall).
  - Score based on CVaR severity: deeper expected shortfall = higher score.

Score = clip(-CVaR_5pct * 7, 0, 100)
  Example: CVaR = -6% -> score = 42
           CVaR = -12% -> score = 84

Sources: World Bank WDI. Method: Artzner et al. (1999) coherent risk measures.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class VaRCVaR(LayerBase):
    layer_id = "lRI"
    name = "VaR / CVaR"

    _CONFIDENCE = 0.05  # 5% left tail

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
              AND dp.value IS NOT NULL
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 15:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient GDP growth data (need >= 15 obs)",
            }

        growth = np.array([float(r["value"]) for r in rows])
        dates = [r["date"] for r in rows]

        alpha = self._CONFIDENCE
        var_5 = float(np.percentile(growth, alpha * 100))
        tail_obs = growth[growth <= var_5]
        cvar_5 = float(np.mean(tail_obs)) if len(tail_obs) > 0 else var_5

        # Parametric VaR (normal approximation) for comparison
        mu = float(np.mean(growth))
        sigma = float(np.std(growth, ddof=1))
        from scipy.stats import norm
        var_5_normal = float(norm.ppf(alpha, loc=mu, scale=sigma))
        cvar_5_normal = float(mu - sigma * norm.pdf(norm.ppf(alpha)) / alpha)

        score = float(np.clip(-cvar_5 * 7, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(growth),
            "period": f"{dates[0]} to {dates[-1]}",
            "confidence_level": f"{int((1 - alpha) * 100)}%",
            "historical": {
                "var_5pct": round(var_5, 4),
                "cvar_5pct": round(cvar_5, 4),
                "n_tail_obs": int(len(tail_obs)),
            },
            "parametric_normal": {
                "var_5pct": round(var_5_normal, 4),
                "cvar_5pct": round(cvar_5_normal, 4),
                "mean": round(mu, 4),
                "std": round(sigma, 4),
            },
        }
