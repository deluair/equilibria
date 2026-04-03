"""Human Capital Economic Return.

Measures whether education investment is translating into growth. High education
spending with low or negative growth = return failure. Computes correlation between
education expenditure and GDP per capita growth over available years.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

INDICATORS = {
    "education_spend": "SE.XPD.TOTL.GD.ZS",   # Govt education expenditure (% of GDP)
    "gdp_pc_growth": "NY.GDP.PCAP.KD.ZG",      # GDP per capita growth (annual %)
}

MIN_OBS = 5

# Reference benchmarks
HIGH_EDUCATION_THRESHOLD = 5.0   # % of GDP
LOW_GROWTH_THRESHOLD = 2.0       # % per capita


class HumanCapitalEconomicReturn(LayerBase):
    layer_id = "l6"
    name = "Human Capital Economic Return"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback", 20)

        series = await self._fetch_series(db, country_iso3, lookback)
        ed = series.get("education_spend", [])
        growth = series.get("gdp_pc_growth", [])

        n = min(len(ed), len(growth))

        # Current-year point estimates
        latest_ed = ed[-1] if ed else None
        latest_growth = growth[-1] if growth else None

        if latest_ed is None or latest_growth is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "country_iso3": country_iso3,
                "reason": "Insufficient data for education spend or GDP per capita growth",
            }

        # Return failure: high education spend but low/negative growth
        high_spend = latest_ed >= HIGH_EDUCATION_THRESHOLD
        low_return = latest_growth < LOW_GROWTH_THRESHOLD

        # Correlation over time series
        if n >= MIN_OBS:
            e_arr = np.array(ed[-n:])
            g_arr = np.array(growth[-n:])
            if np.std(e_arr) > 1e-6 and np.std(g_arr) > 1e-6:
                corr = float(np.corrcoef(e_arr, g_arr)[0, 1])
            else:
                corr = 0.0
        else:
            corr = 0.0

        # Score construction:
        # Base: negative correlation = bad (spend not linked to growth) -> stress
        corr_stress = float(np.clip((1.0 - corr) / 2.0 * 100.0, 0.0, 100.0))

        # Return failure penalty
        if high_spend and low_return:
            return_failure_penalty = 25.0
        elif not high_spend and low_return:
            return_failure_penalty = 10.0
        else:
            return_failure_penalty = 0.0

        # Normalize growth to stress (below LOW_GROWTH_THRESHOLD = stress)
        growth_stress = float(np.clip(
            (LOW_GROWTH_THRESHOLD - latest_growth) / (LOW_GROWTH_THRESHOLD + 5.0) * 100.0,
            0.0, 100.0,
        ))

        score = float(np.clip(
            0.40 * corr_stress + 0.35 * growth_stress + 0.25 * return_failure_penalty,
            0.0, 100.0,
        ))

        await self._store_result(
            db, country_iso3, score, latest_ed, latest_growth,
            corr, return_failure_penalty, high_spend, low_return,
        )

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "return_failure": high_spend and low_return,
            "education_spend_pct_gdp": round(latest_ed, 4),
            "gdp_pc_growth_pct": round(latest_growth, 4),
            "education_growth_correlation": round(corr, 4) if n >= MIN_OBS else None,
            "correlation_obs": n if n >= MIN_OBS else 0,
            "high_education_spend": high_spend,
            "low_growth": low_return,
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology": (
                "Education spend vs GDP per capita growth. Negative correlation + "
                "high spend / low growth = return failure. Score = correlation stress + "
                "growth stress + return failure penalty."
            ),
        }

    async def _fetch_series(
        self, db, country_iso3: str, lookback: int
    ) -> dict[str, list[float]]:
        result = {}
        for key, indicator_id in INDICATORS.items():
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_points dp
                JOIN data_series ds ON dp.series_id = ds.id
                JOIN countries c ON ds.country_id = c.id
                WHERE c.iso3 = ? AND ds.indicator_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY dp.year ASC LIMIT ?
                """,
                (country_iso3, indicator_id, lookback),
            )
            if rows:
                result[key] = [float(r["value"]) for r in rows]
        return result

    async def _store_result(
        self, db, country_iso3: str, score: float,
        ed: float, growth: float, corr: float,
        penalty: float, high_spend: bool, low_return: bool,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results
              (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "human_capital_economic_return",
                country_iso3,
                "l6",
                json.dumps({
                    "high_education_threshold": HIGH_EDUCATION_THRESHOLD,
                    "low_growth_threshold": LOW_GROWTH_THRESHOLD,
                }),
                json.dumps({
                    "education_spend": round(ed, 4),
                    "gdp_pc_growth": round(growth, 4),
                    "correlation": round(corr, 4),
                    "return_failure_penalty": round(penalty, 2),
                    "return_failure": high_spend and low_return,
                }),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
