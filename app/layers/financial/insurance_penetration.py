"""Insurance penetration and financial resilience.

Uses domestic credit to private sector (% GDP, FS.AST.DOMS.GD.ZS) as a
financial depth proxy when insurance-specific data are unavailable. Shallow
financial systems imply limited insurance capacity and lower household
resilience to shocks.

Score (0-100): max(0, 50 - financial_depth * 0.3).
Low financial depth raises the stress score.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InsurancePenetration(LayerBase):
    layer_id = "l7"
    name = "Insurance Penetration"

    _INDICATORS = [
        "FS.AST.DOMS.GD.ZS",   # Domestic credit to private sector (% GDP)
        "FB.BNK.CAPA.ZS",       # Bank capital to assets ratio (fallback)
    ]

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('FS.AST.DOMS.GD.ZS', 'FB.BNK.CAPA.ZS')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.indicator_code, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no financial depth data",
            }

        # Group by indicator, prefer FS.AST.DOMS.GD.ZS
        by_indicator: dict[str, list[float]] = {}
        for r in rows:
            code = r["indicator_code"]
            by_indicator.setdefault(code, []).append(float(r["value"]))

        indicator_used = None
        values = None
        for code in self._INDICATORS:
            if code in by_indicator and by_indicator[code]:
                values = np.array(by_indicator[code])
                indicator_used = code
                break

        if values is None or len(values) == 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no usable financial depth series found",
            }

        financial_depth = float(np.mean(values))
        depth_latest = float(values[-1])

        score = float(np.clip(50.0 - financial_depth * 0.3, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "financial_depth_proxy": {
                "indicator": indicator_used,
                "latest_pct_gdp": round(depth_latest, 2),
                "mean_pct_gdp": round(financial_depth, 2),
                "observations": len(values),
            },
            "insurance_capacity": (
                "low" if financial_depth < 30
                else "moderate" if financial_depth < 70
                else "high"
            ),
        }
