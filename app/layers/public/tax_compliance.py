"""Tax revenue adequacy and compliance gap.

Tax revenue as a share of GDP measures fiscal capacity and the degree to
which the state can fund public services. Below 15% of GDP is considered
the minimum threshold for basic state functions (IMF, World Bank). Low
tax revenue reflects weak compliance, large informal sectors, narrow bases,
and/or high evasion -- all signals of institutional fragility.

Formula: score = clip(max(0, 20 - tax_gdp) * 5, 0, 100).
  At tax/GDP = 20%+: score = 0 (adequate capacity).
  At tax/GDP = 0%: score = 100 (no fiscal capacity).
  At 15% (IMF threshold): score = 25 (watch zone).

High score = low tax capacity = high compliance/governance stress.

References:
    IMF (2011). Revenue Mobilization in Developing Countries. Washington DC.
    Gaspar, V. et al. (2016). Tax capacity and growth: is there a tipping
        point? IMF WP/16/234.
    OECD (2023). Revenue Statistics 2023.

Sources: WDI 'GC.TAX.TOTL.GD.ZS' (tax revenue % GDP).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class TaxCompliance(LayerBase):
    layer_id = "l10"
    name = "Tax Compliance"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score fiscal capacity from tax revenue / GDP.

        Stress rises below 20% and maxes out at 0% tax/GDP.
        """
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.TAX.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no tax revenue data (GC.TAX.TOTL.GD.ZS)",
            }

        latest = rows[0]
        tax_gdp = float(latest["value"])
        year = latest["date"][:4]

        score = float(min(max((20.0 - tax_gdp) * 5.0, 0.0), 100.0))

        capacity_tier = (
            "high capacity" if tax_gdp >= 20
            else "adequate" if tax_gdp >= 15
            else "below threshold" if tax_gdp >= 10
            else "critical"
        )

        return {
            "score": score,
            "results": {
                "country": country,
                "year": year,
                "tax_pct_gdp": tax_gdp,
                "imf_minimum_threshold": 15.0,
                "adequate_threshold": 20.0,
                "capacity_tier": capacity_tier,
                "below_imf_threshold": tax_gdp < 15.0,
                "gap_to_imf_threshold": max(0.0, 15.0 - tax_gdp),
            },
        }
