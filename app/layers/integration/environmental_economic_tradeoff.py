"""Environmental-Economic Tradeoff (EKC Position).

Assesses whether a country is above or below the Environmental Kuznets Curve
turning point. Above turning point = environment-growth tradeoff locked in.
Score reflects severity of that tradeoff.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

INDICATORS = {
    "co2_per_capita": "EN.ATM.CO2E.PC",   # CO2 emissions (metric tons per capita)
    "gdp_per_capita": "NY.GDP.PCAP.KD",   # GDP per capita (constant 2015 USD)
}

# EKC turning point: empirical estimates cluster around $8,000-15,000 (2015 USD)
EKC_TURNING_POINT_GDP = 10000.0  # USD per capita

# CO2 intensity thresholds (metric tons per capita)
CO2_HIGH = 15.0    # clearly above EKC (fossil-heavy)
CO2_LOW = 2.0      # clearly below EKC


class EnvironmentalEconomicTradeoff(LayerBase):
    layer_id = "l6"
    name = "Environmental-Economic Tradeoff"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback", 10)

        raw = await self._fetch_latest(db, country_iso3)
        series = await self._fetch_series(db, country_iso3, lookback)

        co2 = raw.get("co2_per_capita")
        gdp = raw.get("gdp_per_capita")

        if co2 is None or gdp is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "country_iso3": country_iso3,
                "reason": "CO2 or GDP per capita data unavailable",
            }

        # EKC position: above turning point AND still high emissions = tradeoff locked in
        above_turning_point = gdp > EKC_TURNING_POINT_GDP
        co2_stress = float(np.clip((co2 - CO2_LOW) / (CO2_HIGH - CO2_LOW), 0.0, 1.0))

        # Trend: is CO2 declining despite growth (decoupling)?
        co2_series = series.get("co2_per_capita", [])
        decoupling_bonus = 0.0
        if len(co2_series) >= 5:
            slope = float(np.polyfit(np.arange(len(co2_series)), co2_series, 1)[0])
            # Declining CO2 trend = decoupling, reduces score
            if slope < 0:
                decoupling_bonus = float(np.clip(abs(slope) / CO2_HIGH * 100.0, 0.0, 30.0))

        # Base score: high CO2 = high stress; above turning point amplifies
        base_score = co2_stress * 100.0
        if above_turning_point:
            # Above turning point but still high CO2: locked-in tradeoff
            base_score = base_score * 1.20
        else:
            # Below turning point: still on the upswing, slightly less locked in
            base_score = base_score * 0.90

        score = float(np.clip(base_score - decoupling_bonus, 0.0, 100.0))

        ekc_position = "above_turning_point" if above_turning_point else "below_turning_point"

        await self._store_result(db, country_iso3, score, co2, gdp, ekc_position, decoupling_bonus)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "ekc_position": ekc_position,
            "co2_per_capita": round(co2, 4),
            "gdp_per_capita": round(gdp, 2),
            "co2_stress_component": round(co2_stress * 100.0, 2),
            "decoupling_bonus": round(decoupling_bonus, 2),
            "ekc_turning_point_gdp": EKC_TURNING_POINT_GDP,
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology": (
                "EKC position: CO2/capita vs income. Above turning point with high CO2 "
                "= tradeoff locked in. Declining CO2 trend grants decoupling discount."
            ),
        }

    async def _fetch_latest(
        self, db, country_iso3: str
    ) -> dict[str, float | None]:
        result = {}
        for key, indicator_id in INDICATORS.items():
            row = await db.fetch_one(
                """
                SELECT dp.value
                FROM data_points dp
                JOIN data_series ds ON dp.series_id = ds.id
                JOIN countries c ON ds.country_id = c.id
                WHERE c.iso3 = ? AND ds.indicator_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY dp.year DESC LIMIT 1
                """,
                (country_iso3, indicator_id),
            )
            result[key] = float(row["value"]) if row else None
        return result

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
        co2: float, gdp: float, position: str, decoupling_bonus: float,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results
              (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "environmental_economic_tradeoff",
                country_iso3,
                "l6",
                json.dumps({"ekc_turning_point_gdp": EKC_TURNING_POINT_GDP}),
                json.dumps({
                    "co2_per_capita": round(co2, 4),
                    "gdp_per_capita": round(gdp, 2),
                    "ekc_position": position,
                    "decoupling_bonus": round(decoupling_bonus, 2),
                }),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
