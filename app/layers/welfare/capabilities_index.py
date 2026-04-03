"""Capabilities Index module.

Amartya Sen's capabilities approach: measures welfare via health, education,
and income dimensions. Low composite across dimensions = capability deprivation.

Indicators:
  - SP.DYN.LE00.IN  : life expectancy at birth (years)
  - SE.XPD.TOTL.GD.ZS : education expenditure (% of GDP)
  - NY.GDP.PCAP.KD  : GDP per capita (constant 2015 USD)

Each dimension normalized relative to global benchmarks, then combined.
Score = 100 - (composite_capability * 100), so low capability = high stress score.

Sources: WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# Normalization goalposts (min, max) per Sen/UNDP conventions
_LIFE_EXP_MIN = 20.0
_LIFE_EXP_MAX = 85.0
_EDUC_SPEND_MIN = 0.5
_EDUC_SPEND_MAX = 10.0
_GDPPC_MIN = 100.0
_GDPPC_MAX = 75000.0


def _normalize(value: float, lo: float, hi: float) -> float:
    return float(np.clip((value - lo) / (hi - lo), 0.0, 1.0))


class CapabilitiesIndex(LayerBase):
    layer_id = "lWE"
    name = "Capabilities Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        series_ids = {
            "life_expectancy": "SP.DYN.LE00.IN",
            "education_spend": "SE.XPD.TOTL.GD.ZS",
            "gdp_per_capita": "NY.GDP.PCAP.KD",
        }

        latest: dict[str, float | None] = {}
        dates: dict[str, str | None] = {}

        for key, sid in series_ids.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country, sid),
            )
            if rows:
                latest[key] = float(rows[0]["value"])
                dates[key] = rows[0]["date"]
            else:
                latest[key] = None
                dates[key] = None

        available = {k: v for k, v in latest.items() if v is not None}
        if len(available) == 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no capability dimension data available",
            }

        dims: dict[str, float] = {}
        if latest["life_expectancy"] is not None:
            dims["health"] = _normalize(latest["life_expectancy"], _LIFE_EXP_MIN, _LIFE_EXP_MAX)
        if latest["education_spend"] is not None:
            dims["education"] = _normalize(latest["education_spend"], _EDUC_SPEND_MIN, _EDUC_SPEND_MAX)
        if latest["gdp_per_capita"] is not None:
            dims["income"] = _normalize(np.log(max(latest["gdp_per_capita"], _GDPPC_MIN)),
                                        np.log(_GDPPC_MIN), np.log(_GDPPC_MAX))

        composite_capability = float(np.mean(list(dims.values())))
        score = float(np.clip((1.0 - composite_capability) * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "composite_capability": round(composite_capability, 4),
            "dimensions": {k: round(v, 4) for k, v in dims.items()},
            "life_expectancy": round(latest["life_expectancy"], 2) if latest["life_expectancy"] is not None else None,
            "life_expectancy_date": dates["life_expectancy"],
            "education_spend_pct_gdp": round(latest["education_spend"], 2) if latest["education_spend"] is not None else None,
            "education_spend_date": dates["education_spend"],
            "gdp_per_capita_usd": round(latest["gdp_per_capita"], 2) if latest["gdp_per_capita"] is not None else None,
            "gdp_per_capita_date": dates["gdp_per_capita"],
            "n_dimensions": len(dims),
            "method": "Sen capabilities approach; each dimension normalized to [0,1]; score = (1 - mean) * 100",
            "reference": "Sen 1985, 1999; UNDP HDR normalization goalposts",
        }
