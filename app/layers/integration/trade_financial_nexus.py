"""Trade-Financial Nexus.

Measures co-movement and synchronization stress between trade integration
and financial integration. Both declining = economic disintegration.
Both volatile = synchronization stress.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

INDICATORS = {
    "trade_openness": "NE.TRD.GNFS.ZS",         # Trade (% of GDP)
    "fdi_inflows": "BX.KLT.DINV.WD.GD.ZS",      # FDI net inflows (% of GDP)
}

MIN_OBS = 5


class TradeFinancialNexus(LayerBase):
    layer_id = "l6"
    name = "Trade-Financial Nexus"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        lookback = kwargs.get("lookback", 15)

        series = await self._fetch_series(db, country_iso3, lookback)
        trade = series.get("trade_openness", [])
        fdi = series.get("fdi_inflows", [])

        n = min(len(trade), len(fdi))
        if n < MIN_OBS:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "country_iso3": country_iso3,
                "reason": f"Insufficient paired observations: {n} < {MIN_OBS}",
            }

        t = np.array(trade[-n:])
        f = np.array(fdi[-n:])

        # Trend: declining integration stress
        trade_trend = float(np.polyfit(np.arange(n), t, 1)[0])
        fdi_trend = float(np.polyfit(np.arange(n), f, 1)[0])
        disintegration_stress = self._disintegration_score(trade_trend, fdi_trend)

        # Volatility: coefficient of variation (normalize to 0-100)
        trade_cv = float(np.std(t) / abs(np.mean(t))) if abs(np.mean(t)) > 1e-6 else 0.0
        fdi_cv = float(np.std(f) / abs(np.mean(f))) if abs(np.mean(f)) > 1e-6 else 0.0
        sync_stress = float(np.clip((trade_cv + fdi_cv) / 2.0 * 100.0, 0.0, 100.0))

        # Correlation: low positive or negative = decoupling stress
        if np.std(t) > 1e-6 and np.std(f) > 1e-6:
            corr = float(np.corrcoef(t, f)[0, 1])
        else:
            corr = 0.0
        # Negative or zero correlation = decoupling (stress); perfect positive = no stress
        decoupling_stress = float(np.clip((1.0 - corr) / 2.0 * 100.0, 0.0, 100.0))

        score = float(np.clip(
            0.35 * disintegration_stress + 0.35 * sync_stress + 0.30 * decoupling_stress,
            0.0, 100.0,
        ))

        await self._store_result(
            db, country_iso3, score,
            disintegration_stress, sync_stress, decoupling_stress, corr,
            trade_trend, fdi_trend,
        )

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "disintegration_stress": round(disintegration_stress, 2),
            "synchronization_stress": round(sync_stress, 2),
            "decoupling_stress": round(decoupling_stress, 2),
            "trade_fdi_correlation": round(corr, 4),
            "trade_trend_slope": round(trade_trend, 4),
            "fdi_trend_slope": round(fdi_trend, 4),
            "observations": n,
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "methodology": (
                "Three stress dimensions: (1) both declining = disintegration, "
                "(2) joint volatility = synchronization stress, "
                "(3) low correlation = decoupling stress."
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

    @staticmethod
    def _disintegration_score(trade_trend: float, fdi_trend: float) -> float:
        """Both declining = high stress (100), both rising = low stress (0)."""
        # Scale each trend to a stress: negative slope -> stress
        # Bound slopes to [-5, 5] range
        def slope_to_stress(s: float) -> float:
            return float(np.clip((-s + 5.0) / 10.0 * 100.0, 0.0, 100.0))

        return (slope_to_stress(trade_trend) + slope_to_stress(fdi_trend)) / 2.0

    async def _store_result(
        self, db, country_iso3: str, score: float,
        disint: float, sync: float, decoup: float,
        corr: float, trade_trend: float, fdi_trend: float,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results
              (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "trade_financial_nexus",
                country_iso3,
                "l6",
                json.dumps({"indicators": INDICATORS}),
                json.dumps({
                    "disintegration_stress": round(disint, 2),
                    "synchronization_stress": round(sync, 2),
                    "decoupling_stress": round(decoup, 2),
                    "correlation": round(corr, 4),
                    "trade_trend": round(trade_trend, 4),
                    "fdi_trend": round(fdi_trend, 4),
                }),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
