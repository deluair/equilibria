"""Agricultural interest burden: real interest rates vs farm margin sustainability.

Methodology
-----------
**Interest burden on farmers** estimated from:
    - FR.INR.RINR: Real interest rate (%) -- the cost of borrowing adjusted for
      inflation. High real rates erode farm margins directly: a 10% real rate on
      working capital loans can consume the entire net margin of small farmers.

    Benchmark (World Bank / FAO):
        real rate < 2%  -> benign (low burden)
        real rate 2-6%  -> moderate
        real rate 6-12% -> elevated
        real rate > 12% -> crisis-level for agriculture

    score = normalised stress index:
        score = min(100, max(0, (real_rate - 2) * (100/18)))
        -> 0 at 2%, 100 at 20%

Score (0-100): higher = worse interest burden on agricultural borrowers.

Sources: World Bank WDI (FR.INR.RINR)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase

_SQL = """
    SELECT value FROM data_points
    WHERE series_id = (
        SELECT id FROM data_series
        WHERE indicator_code = ? OR name LIKE ?
    )
    ORDER BY date DESC LIMIT 15
"""

_BENIGN_THRESHOLD = 2.0    # % real rate below which burden is low
_SCALE_RANGE = 18.0        # ppt range mapping to 0-100 score (2% -> 20%)


class AgriculturalInterestBurden(LayerBase):
    layer_id = "lAF"
    name = "Agricultural Interest Burden"

    async def compute(self, db, **kwargs) -> dict:
        code_ir, name_ir = "FR.INR.RINR", "%real interest rate%"

        rows = await db.fetch_all(_SQL, (code_ir, name_ir))

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no real interest rate data"}

        vals = [float(r["value"]) for r in rows if r["value"] is not None]

        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable interest rate values"}

        real_rate = statistics.mean(vals[:3])
        trend = None
        if len(vals) >= 5:
            recent = statistics.mean(vals[:3])
            older = statistics.mean(vals[3:6]) if len(vals) >= 6 else vals[3]
            trend = "rising" if recent > older + 0.5 else "falling" if recent < older - 0.5 else "stable"

        score = max(0.0, min(100.0, (real_rate - _BENIGN_THRESHOLD) * (100.0 / _SCALE_RANGE)))

        burden_level = (
            "low" if real_rate < _BENIGN_THRESHOLD
            else "moderate" if real_rate < 6.0
            else "elevated" if real_rate < 12.0
            else "crisis"
        )

        return {
            "score": round(score, 2),
            "metrics": {
                "real_interest_rate_pct": round(real_rate, 2),
                "burden_level": burden_level,
                "trend": trend,
                "observations_used": len(vals),
            },
        }
