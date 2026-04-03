"""Innovation-Growth Premium.

Measures the R&D to growth chain. Computes R&D elasticity of per capita growth.
Low elasticity = innovation-growth disconnect.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

INDICATORS = {
    "rnd_spend": "GB.XPD.RSDV.GD.ZS",    # R&D expenditure (% of GDP)
    "gdp_pc_growth": "NY.GDP.PCAP.KD.ZG", # GDP per capita growth (annual %)
}

MIN_OBS = 6

# Reference: OECD high-innovation economies average ~2.5% R&D/GDP
HIGH_RND_THRESHOLD = 2.0   # % of GDP
LOW_ELASTICITY_THRESHOLD = 0.5  # growth % per 1% R&D/GDP


class InnovationGrowthPremium(LayerBase):
    layer_id = "l6"
    name = "Innovation-Growth Premium"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback", 20)

        series = await self._fetch_series(db, country_iso3, lookback)
        rnd = series.get("rnd_spend", [])
        growth = series.get("gdp_pc_growth", [])

        n = min(len(rnd), len(growth))

        latest_rnd = rnd[-1] if rnd else None
        latest_growth = growth[-1] if growth else None

        if latest_rnd is None or latest_growth is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "country_iso3": country_iso3,
                "reason": "Insufficient R&D spend or GDP per capita growth data",
            }

        # Compute R&D elasticity via OLS slope (growth on R&D)
        elasticity = None
        corr = 0.0
        if n >= MIN_OBS:
            r_arr = np.array(rnd[-n:])
            g_arr = np.array(growth[-n:])
            if np.std(r_arr) > 1e-6:
                slope, _ = np.polyfit(r_arr, g_arr, 1)
                elasticity = float(slope)
                if np.std(g_arr) > 1e-6:
                    corr = float(np.corrcoef(r_arr, g_arr)[0, 1])

        # Score components
        # 1. Disconnect stress: high R&D but low elasticity
        high_rnd = latest_rnd >= HIGH_RND_THRESHOLD
        if elasticity is not None:
            disconnect = elasticity < LOW_ELASTICITY_THRESHOLD
            # Normalize elasticity: negative or zero = full stress, HIGH = no stress
            elast_stress = float(np.clip(
                (LOW_ELASTICITY_THRESHOLD - elasticity) / (LOW_ELASTICITY_THRESHOLD + 2.0) * 100.0,
                0.0, 100.0,
            ))
        else:
            disconnect = False
            elast_stress = 50.0  # neutral when no data

        # 2. R&D level: low R&D = innovation gap
        rnd_gap_stress = float(np.clip(
            (HIGH_RND_THRESHOLD - latest_rnd) / HIGH_RND_THRESHOLD * 100.0,
            0.0, 100.0,
        ))

        # 3. Correlation stress: negative correlation = innovation-growth disconnect
        corr_stress = float(np.clip((1.0 - corr) / 2.0 * 100.0, 0.0, 100.0))

        score = float(np.clip(
            0.40 * elast_stress + 0.35 * rnd_gap_stress + 0.25 * corr_stress,
            0.0, 100.0,
        ))

        # Amplify if high R&D but still disconnected (wasted investment)
        if high_rnd and disconnect:
            score = float(np.clip(score * 1.15, 0.0, 100.0))

        await self._store_result(
            db, country_iso3, score,
            latest_rnd, latest_growth, elasticity, corr,
            high_rnd, disconnect,
        )

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "innovation_growth_disconnect": high_rnd and disconnect,
            "rnd_spend_pct_gdp": round(latest_rnd, 4),
            "gdp_pc_growth_pct": round(latest_growth, 4),
            "rnd_growth_elasticity": round(elasticity, 4) if elasticity is not None else None,
            "rnd_growth_correlation": round(corr, 4) if n >= MIN_OBS else None,
            "elasticity_obs": n if n >= MIN_OBS else 0,
            "high_rnd_spend": high_rnd,
            "low_elasticity": disconnect,
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology": (
                "R&D elasticity of per capita growth via OLS. "
                "High R&D + low elasticity = innovation-growth disconnect. "
                "Score = elasticity stress + R&D gap + correlation disconnect."
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
        rnd: float, growth: float,
        elasticity: float | None, corr: float,
        high_rnd: bool, disconnect: bool,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results
              (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "innovation_growth_premium",
                country_iso3,
                "l6",
                json.dumps({
                    "high_rnd_threshold": HIGH_RND_THRESHOLD,
                    "low_elasticity_threshold": LOW_ELASTICITY_THRESHOLD,
                }),
                json.dumps({
                    "rnd_spend": round(rnd, 4),
                    "gdp_pc_growth": round(growth, 4),
                    "rnd_growth_elasticity": round(elasticity, 4) if elasticity is not None else None,
                    "rnd_growth_correlation": round(corr, 4),
                    "innovation_growth_disconnect": high_rnd and disconnect,
                }),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
