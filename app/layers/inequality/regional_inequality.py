"""Regional Inequality module.

Proxies urban-rural income gap using urbanization rate and GDP per capita.

High urbanization with low GDP per capita signals that cities are absorbing
rural migrants without proportional income gains, indicating a structural
urban-rural gap. Low urbanization combined with low income suggests rural
populations are excluded from growth entirely.

Indicators:
- SP.URB.TOTL.IN.ZS: Urban population (% of total population)
- NY.GDP.PCAP.KD: GDP per capita (constant 2015 USD)

Score logic:
- Base: inverse of log-normalized income (poor countries score higher)
- Modifier: urbanization gap from 50% threshold amplifies score when
  income is low, because extreme urbanization at low income = acute gap.

Score = income_penalty * urbanization_modifier, clipped to 0-100.

Sources: World Bank WDI (SP.URB.TOTL.IN.ZS, NY.GDP.PCAP.KD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RegionalInequality(LayerBase):
    layer_id = "lIQ"
    name = "Regional Inequality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        urban_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not urban_rows and not gdp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        urbanization = float(urban_rows[0]["value"]) if urban_rows else 50.0
        gdp_pc = float(gdp_rows[0]["value"]) if gdp_rows else 10000.0
        has_urban = bool(urban_rows)
        has_gdp = bool(gdp_rows)

        # Income penalty: log scale, $1000 = ~70 pts, $10k = ~35 pts, $50k = ~10 pts
        log_gdp = float(np.log(max(gdp_pc, 100.0)))
        log_max = float(np.log(80000.0))
        income_penalty = float(np.clip((1.0 - log_gdp / log_max) * 80.0, 0, 80))

        # Urbanization modifier: deviation from 50% amplifies regional gaps
        # Very low (<30%) or very high (>80%) urbanization at low income = bigger gap
        urban_dev = abs(urbanization - 50.0) / 50.0
        if gdp_pc < 5000.0:
            urban_modifier = 1.0 + urban_dev * 0.5
        else:
            urban_modifier = 1.0

        score = float(np.clip(income_penalty * urban_modifier, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "urbanization_pct": round(urbanization, 2),
            "gdp_per_capita_usd": round(gdp_pc, 0),
            "urban_source": "observed" if has_urban else "imputed_default",
            "gdp_source": "observed" if has_gdp else "imputed_default",
            "income_penalty": round(income_penalty, 2),
            "urban_modifier": round(urban_modifier, 4),
            "interpretation": {
                "low_income": gdp_pc < 5000,
                "high_urbanization": urbanization > 70,
                "low_urbanization": urbanization < 30,
            },
        }
