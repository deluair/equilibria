"""Economic geography index: market access composite.

Market access is a composite of trade openness, population density, and
GDP per capita. Countries with low values on all three face structural
geographic disadvantage in participating in global markets.

Components:
    1. Trade openness: NE.TRD.GNFS.ZS (% of GDP)
    2. Population density: EN.POP.DNST (people per km²)
    3. GDP per capita: NY.GDP.PCAP.KD (constant 2015 USD)

Composite = normalize each to 0-1 using log scale where appropriate,
then average. Score = 100 - composite_normalized * 100.
High score = low market access = geographic disadvantage.

Normalization benchmarks (approximate global ranges):
    Trade openness: 0-200%  -> normalize to 0-1
    Population density: 1-10,000 people/km² -> log scale
    GDP per capita: 500-100,000 USD -> log scale

References:
    Redding, S. & Venables, A.J. (2004). Economic Geography and International
        Inequality. Journal of International Economics, 62(1), 53-82.
    Head, K. & Mayer, T. (2004). The Empirics of Agglomeration and Trade.
        Handbook of Regional and Urban Economics, Vol. 4.

Sources: World Bank WDI NE.TRD.GNFS.ZS, EN.POP.DNST, NY.GDP.PCAP.KD.
"""

from __future__ import annotations

import math

import numpy as np

from app.layers.base import LayerBase

# Benchmark ranges for normalization
_OPENNESS_MAX = 200.0       # % of GDP, upper benchmark
_LOG_DENSITY_MIN = 0.0      # log10(1) = 0
_LOG_DENSITY_MAX = 4.0      # log10(10000) = 4
_LOG_GDPPC_MIN = math.log10(500)
_LOG_GDPPC_MAX = math.log10(100_000)


class EconomicGeographyIndex(LayerBase):
    layer_id = "l11"
    name = "Economic Geography Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        openness_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.TRD.GNFS.ZS'
            ORDER BY dp.date DESC
            LIMIT 3
            """,
            (country,),
        )

        density_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EN.POP.DNST'
            ORDER BY dp.date DESC
            LIMIT 3
            """,
            (country,),
        )

        gdppc_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date DESC
            LIMIT 3
            """,
            (country,),
        )

        available = []
        openness = density = gdppc = None
        openness_norm = density_norm = gdppc_norm = None

        if openness_rows:
            openness = float(openness_rows[0]["value"])
            openness_norm = float(np.clip(openness / _OPENNESS_MAX, 0.0, 1.0))
            available.append(openness_norm)

        if density_rows:
            density = float(density_rows[0]["value"])
            if density > 0:
                log_d = math.log10(max(density, 1.0))
                density_norm = float(np.clip(
                    (log_d - _LOG_DENSITY_MIN) / (_LOG_DENSITY_MAX - _LOG_DENSITY_MIN), 0.0, 1.0
                ))
                available.append(density_norm)

        if gdppc_rows:
            gdppc = float(gdppc_rows[0]["value"])
            if gdppc > 0:
                log_g = math.log10(max(gdppc, 500.0))
                gdppc_norm = float(np.clip(
                    (log_g - _LOG_GDPPC_MIN) / (_LOG_GDPPC_MAX - _LOG_GDPPC_MIN), 0.0, 1.0
                ))
                available.append(gdppc_norm)

        if not available:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no market access component data",
                "country": country,
            }

        composite = float(np.mean(available))
        score = float(np.clip(100.0 - composite * 100.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "trade_openness_pct": round(openness, 2) if openness is not None else None,
            "population_density_per_km2": round(density, 2) if density is not None else None,
            "gdp_per_capita_usd": round(gdppc, 2) if gdppc is not None else None,
            "openness_normalized": round(openness_norm, 4) if openness_norm is not None else None,
            "density_normalized": round(density_norm, 4) if density_norm is not None else None,
            "gdppc_normalized": round(gdppc_norm, 4) if gdppc_norm is not None else None,
            "composite_market_access": round(composite, 4),
            "components_available": len(available),
            "market_access_level": (
                "high" if composite > 0.6
                else "moderate" if composite > 0.35
                else "low"
            ),
            "_source": "WDI NE.TRD.GNFS.ZS, EN.POP.DNST, NY.GDP.PCAP.KD",
        }
