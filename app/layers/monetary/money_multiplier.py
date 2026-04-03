"""Money Multiplier: broad money to base money ratio trend and monetary contraction signals.

Methodology
-----------
The money multiplier (m) relates broad money (M2) to the monetary base (H):

    m = M2 / H

In practice, M2/GDP and domestic credit/GDP from WDI provide the proxy:

    multiplier_proxy = M2_gdp / domestic_credit_gdp

A declining multiplier signals:
  - Banks hoarding reserves (credit contraction)
  - Reduced velocity / animal spirits collapse
  - Balance-sheet recession dynamics (Koo, 2011)

Trend estimation: OLS slope on log-multiplier over time.
  slope < 0  -> monetary contraction
  |slope| large -> rapid de-leveraging

Score reflects stress from declining multiplier trend.

Sources: World Bank WDI
  FM.LBL.BMNY.GD.ZS  - Broad money (% of GDP)
  FS.AST.DOMS.GD.ZS  - Domestic credit provided by financial sector (% of GDP)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MoneyMultiplier(LayerBase):
    layer_id = "l15"
    name = "Money Multiplier"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 20)

        series_map = {
            "broad_money_gdp": f"FM.LBL.BMNY.GD.ZS_{country}",
            "domestic_credit_gdp": f"FS.AST.DOMS.GD.ZS_{country}",
        }

        data: dict[str, dict[str, float]] = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE series_id = ?) "
                "AND date >= date('now', ?) ORDER BY date",
                (code, f"-{lookback} years"),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        if not data.get("broad_money_gdp") or not data.get("domestic_credit_gdp"):
            return {"score": 50.0, "results": {"error": "insufficient data for money multiplier"}}

        common = sorted(set(data["broad_money_gdp"]) & set(data["domestic_credit_gdp"]))
        if len(common) < 5:
            return {"score": 50.0, "results": {"error": "too few overlapping observations"}}

        m2 = np.array([data["broad_money_gdp"][d] for d in common])
        dc = np.array([data["domestic_credit_gdp"][d] for d in common])

        # Guard against division by zero
        dc_safe = np.where(dc > 1e-6, dc, np.nan)
        multiplier = m2 / dc_safe
        valid_mask = np.isfinite(multiplier)
        multiplier_valid = multiplier[valid_mask]
        t_valid = np.arange(len(common))[valid_mask].astype(float)

        if len(multiplier_valid) < 5:
            return {"score": 50.0, "results": {"error": "insufficient valid multiplier observations"}}

        log_mult = np.log(np.maximum(multiplier_valid, 1e-10))
        trend_coef = np.polyfit(t_valid, log_mult, 1)
        slope = float(trend_coef[0])  # per observation
        # Annualise if data is annual (each step ~ 1 year)
        annual_slope_pct = slope * 100.0

        current = float(multiplier_valid[-1])
        mean_mult = float(np.mean(multiplier_valid))
        std_mult = float(np.std(multiplier_valid, ddof=1))

        results: dict = {
            "country": country,
            "n_obs": int(len(multiplier_valid)),
            "period": f"{common[0]} to {common[-1]}",
            "multiplier_proxy_latest": round(current, 4),
            "multiplier_proxy_mean": round(mean_mult, 4),
            "multiplier_proxy_std": round(std_mult, 4),
            "trend_slope_log": round(slope, 6),
            "trend_annualized_pct": round(annual_slope_pct, 3),
            "declining": slope < 0,
            "rapid_deleveraging": slope < -0.05,
        }

        # Score: declining multiplier = monetary contraction = stress
        if slope >= 0:
            score = 10.0
        else:
            magnitude = abs(annual_slope_pct)
            score = float(np.clip(magnitude * 10.0, 10.0, 100.0))

        return {"score": round(score, 1), "results": results}
