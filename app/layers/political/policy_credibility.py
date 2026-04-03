"""Policy Credibility module.

Policy consistency: inflation variance + fiscal balance variance as proxy for
policy credibility.

Theory:
    Barro & Gordon (1983) show that credible monetary policy requires commitment
    devices. Fiscal policy credibility similarly depends on consistent budget
    balance behavior. High variance in both inflation and fiscal balance signals
    discretionary, time-inconsistent policymaking -- a core determinant of
    investment risk and sovereign spreads. Alesina & Tabellini (1990) link
    government fragmentation to fiscal volatility.

Indicators:
    - FP.CPI.TOTL.ZG: Inflation (CPI, annual %). Source: WDI.
    - GC.BAL.CASH.GD.ZS: Cash surplus/deficit (% of GDP). Source: WDI.

Score construction:
    inf_cv = std(inflation) / max(1, abs(mean(inflation)))  [coefficient of variation]
    fis_std = std(fiscal_balance)
    inf_stress = clip(inf_cv / 3, 0, 1)
    fis_stress = clip(fis_std / 10, 0, 1)
    score = clip((inf_stress * 0.5 + fis_stress * 0.5) * 100, 0, 100)

References:
    Barro, R. & Gordon, D. (1983). "Rules, Discretion, and Reputation." JME 12.
    Alesina, A. & Tabellini, G. (1990). "A Positive Theory of Fiscal Deficits." RES 57.
    World Bank. (2023). World Development Indicators.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PolicyCredibility(LayerBase):
    layer_id = "l12"
    name = "Policy Credibility"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate policy credibility via macroeconomic variance.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
        """
        country = kwargs.get("country_iso3", "BGD")

        inf_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%FP.CPI.TOTL.ZG%' OR ds.name LIKE '%inflation%consumer%price%'
                   OR ds.name LIKE '%cpi%annual%' OR ds.name LIKE '%inflation%cpi%')
            ORDER BY dp.date
            """,
            (country,),
        )

        fis_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%GC.BAL.CASH.GD.ZS%' OR ds.name LIKE '%cash%surplus%deficit%gdp%'
                   OR ds.name LIKE '%fiscal%balance%gdp%' OR ds.name LIKE '%budget%balance%gdp%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not inf_rows and not fis_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no policy credibility data"}

        inf_stress = 0.5
        inf_detail = None
        if inf_rows:
            inf = np.array([float(r["value"]) for r in inf_rows])
            inf_mean = float(np.mean(inf))
            inf_std = float(np.std(inf))
            inf_cv = inf_std / max(1.0, abs(inf_mean))
            inf_stress = float(np.clip(inf_cv / 3.0, 0, 1))
            inf_detail = {
                "mean_pct": round(inf_mean, 3),
                "std_pct": round(inf_std, 3),
                "coeff_variation": round(inf_cv, 4),
                "n_obs": len(inf),
                "date_range": [str(inf_rows[0]["date"]), str(inf_rows[-1]["date"])],
            }

        fis_stress = 0.5
        fis_detail = None
        if fis_rows:
            fis = np.array([float(r["value"]) for r in fis_rows])
            fis_std = float(np.std(fis))
            fis_stress = float(np.clip(fis_std / 10.0, 0, 1))
            fis_detail = {
                "mean_pct_gdp": round(float(np.mean(fis)), 3),
                "std_pct_gdp": round(fis_std, 3),
                "n_obs": len(fis),
                "date_range": [str(fis_rows[0]["date"]), str(fis_rows[-1]["date"])],
            }

        score = float(np.clip(
            (inf_stress * 0.5 + fis_stress * 0.5) * 100,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "inflation_volatility_stress": round(inf_stress * 0.5 * 100, 2),
                "fiscal_volatility_stress": round(fis_stress * 0.5 * 100, 2),
            },
            "credibility_tier": (
                "low" if score > 65 else "moderate" if score > 35 else "high"
            ),
            "reference": "Barro & Gordon 1983; Alesina & Tabellini 1990; WDI",
        }

        if inf_detail:
            result["inflation_variance"] = inf_detail
        if fis_detail:
            result["fiscal_balance_variance"] = fis_detail

        return result
