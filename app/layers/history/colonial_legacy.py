"""Colonial Legacy module.

Measures the institutional quality gap: governance (Rule of Law) relative to
income level. Countries with weak governance despite moderate-to-high per
capita income signal a persistent colonial legacy trap.

Indicators:
  RL.EST   - Rule of Law estimate (WGI, typically -2.5 to +2.5)
  NY.GDP.PCAP.KD - GDP per capita, constant USD

Method: Expected governance = f(log income). Residual gap scores stress.
Score: large negative residual (governance worse than income predicts) -> high.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase

# Approximate cross-country OLS coefficients (governance ~ log GDP per capita).
# These represent well-established stylised facts from WGI/WDI literature.
# Slope ~0.9, intercept ~-8.0 in log scale.
_BENCHMARK_SLOPE = 0.90
_BENCHMARK_INTERCEPT = -8.0


class ColonialLegacy(LayerBase):
    layer_id = "lHI"
    name = "Colonial Legacy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rl_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'RL.EST'
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

        if not rl_rows or not gdp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        latest_rl = float(rl_rows[0]["value"])
        latest_gdp = float(gdp_rows[0]["value"])

        if latest_gdp <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "non-positive GDP per capita"}

        log_gdp = np.log(latest_gdp)
        predicted_rl = _BENCHMARK_SLOPE * log_gdp + _BENCHMARK_INTERCEPT
        gap = predicted_rl - latest_rl  # positive gap = governance below expectation

        # Score: gap of +1.5 or more on the RL scale (-2.5 to +2.5) = deep trap (100).
        # Gap <= 0 (governance meets or exceeds expectation) = no legacy stress (0).
        score = float(np.clip(gap / 1.5 * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "latest_rule_of_law": round(latest_rl, 4),
            "predicted_rule_of_law": round(predicted_rl, 4),
            "governance_gap": round(gap, 4),
            "log_gdp_pc": round(log_gdp, 4),
            "latest_gdp_pc": round(latest_gdp, 2),
            "note": "Positive gap = governance below income-level expectation (legacy trap)",
        }
