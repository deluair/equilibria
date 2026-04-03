"""Public investment quality via gross fixed capital formation.

Gross fixed capital formation (GFCF) as a share of GDP is a standard proxy
for the economy's investment in physical capital -- machinery, infrastructure,
buildings. Low GFCF signals underinvestment in productive capacity, a key
constraint on long-run growth (Solow, 1956; IMF 2014 WEO Chapter 3).

While GFCF includes private investment, for countries with limited private
sectors it tracks public capital accumulation closely. Below 20% GFCF/GDP
is the IMF/World Bank underinvestment threshold.

Formula: score = clip(max(0, 20 - gfcf_pct) * 5, 0, 100).
  At GFCF = 20%+: score = 0 (adequate investment).
  At GFCF = 0%: score = 100 (no capital formation).
  At GFCF = 15%: score = 25 (watch zone).

High score = low investment = high productive capacity stress.

References:
    Solow, R.M. (1956). A contribution to the theory of economic growth.
        Quarterly Journal of Economics, 70(1), 65-94.
    IMF (2014). World Economic Outlook, Chapter 3: Is it time for an
        infrastructure push? Washington DC.
    Warner, A.M. (2014). Public Investment as an Engine of Growth.
        IMF WP/14/148.

Sources: WDI 'NE.GDI.FTOT.ZS' (GFCF % GDP).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class PublicInvestmentQuality(LayerBase):
    layer_id = "l10"
    name = "Public Investment Quality"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Score underinvestment stress from GFCF % GDP.

        Stress rises below 20% GFCF/GDP threshold and maxes at 0%.
        """
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.GDI.FTOT.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no gross fixed capital formation data (NE.GDI.FTOT.ZS)",
            }

        latest = rows[0]
        gfcf_pct = float(latest["value"])
        year = latest["date"][:4]

        score = float(min(max((20.0 - gfcf_pct) * 5.0, 0.0), 100.0))

        investment_tier = (
            "high investment" if gfcf_pct >= 30
            else "adequate" if gfcf_pct >= 20
            else "below threshold" if gfcf_pct >= 10
            else "critical underinvestment"
        )

        return {
            "score": score,
            "results": {
                "country": country,
                "year": year,
                "gfcf_pct_gdp": gfcf_pct,
                "imf_threshold_pct": 20.0,
                "investment_tier": investment_tier,
                "below_threshold": gfcf_pct < 20.0,
                "gap_to_threshold": max(0.0, 20.0 - gfcf_pct),
            },
        }
