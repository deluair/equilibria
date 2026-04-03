"""Government debt sustainability stress.

High public debt constrains fiscal space, raises borrowing costs, crowds out
productive investment, and increases vulnerability to rollover crises. The
IMF treats debt-to-GDP above 60% for developing economies as a stress zone,
with 90%+ associated with significantly lower growth (Reinhart & Rogoff, 2010;
though the exact threshold is debated, the directionality holds).

This module scores fiscal stress from the debt burden. Stress begins above
40% debt-to-GDP (for low-income countries), with a linear score rising to
100 at 140% debt-to-GDP.

Formula: score = clip((debt_gdp - 40) * 1.0, 0, 100).
  At 40% debt/GDP: score = 0 (no stress).
  At 140% debt/GDP: score = 100 (maximum stress).

High score = high fiscal stress.

References:
    Reinhart, C.M. & Rogoff, K.S. (2010). Growth in a time of debt.
        American Economic Review, 100(2), 573-578.
    IMF (2023). Fiscal Monitor. Washington DC.
    IMF DSA framework for low-income and emerging market economies.

Sources: WDI 'GC.DOD.TOTL.GD.ZS' (general government net debt % GDP).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class PublicDebtBurden(LayerBase):
    layer_id = "l10"
    name = "Public Debt Burden"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score government debt sustainability from debt-to-GDP ratio.

        Stress starts above 40% debt/GDP and hits maximum at 140%.
        """
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.DOD.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no public debt data (GC.DOD.TOTL.GD.ZS)",
            }

        latest = rows[0]
        debt_gdp = float(latest["value"])
        year = latest["date"][:4]

        score = float(min(max(debt_gdp - 40.0, 0.0), 100.0))

        imf_category = (
            "very high stress" if debt_gdp >= 100
            else "high stress" if debt_gdp >= 70
            else "moderate" if debt_gdp >= 40
            else "sustainable"
        )

        return {
            "score": score,
            "results": {
                "country": country,
                "year": year,
                "debt_pct_gdp": debt_gdp,
                "stress_threshold_pct": 40.0,
                "max_stress_pct": 140.0,
                "imf_category": imf_category,
                "above_stress_threshold": debt_gdp > 40.0,
            },
        }
