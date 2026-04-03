"""Early warning composite module.

Multi-indicator early warning system combining external debt (DT.DOD.DECT.GD.ZS),
reserve adequacy (FI.RES.TOTL.MO), current account (BN.CAB.XOKA.GD.ZS),
and private credit (FS.AST.PRVT.GD.ZS). Each indicator breaches a threshold;
the count and severity of breaches determine the composite alarm.

Score (0-100): more threshold breaches at greater severity = higher score.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

INDICATORS = [
    ("DT.DOD.DECT.GD.ZS", "external debt stocks"),
    ("FI.RES.TOTL.MO", "total reserves months of imports"),
    ("BN.CAB.XOKA.GD.ZS", "current account balance"),
    ("FS.AST.PRVT.GD.ZS", "domestic credit private sector"),
]

# (threshold, direction, weight): direction "above" = higher is riskier
THRESHOLDS = {
    "DT.DOD.DECT.GD.ZS": (60.0, "above", 0.30),
    "FI.RES.TOTL.MO": (3.0, "below", 0.30),
    "BN.CAB.XOKA.GD.ZS": (-5.0, "below", 0.20),
    "FS.AST.PRVT.GD.ZS": (80.0, "above", 0.20),
}


class EarlyWarningComposite(LayerBase):
    layer_id = "lFC"
    name = "Early Warning Composite"

    async def compute(self, db, **kwargs) -> dict:
        indicator_data: dict[str, float | None] = {}

        for code, name in INDICATORS:
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (code, f"%{name}%"),
            )
            vals = [float(r["value"]) for r in rows if r["value"] is not None]
            indicator_data[code] = vals[0] if vals else None

        available = {k: v for k, v in indicator_data.items() if v is not None}
        if not available:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no early warning data"}

        breach_details = []
        weighted_score = 0.0
        total_weight = 0.0

        for code, (threshold, direction, weight) in THRESHOLDS.items():
            val = indicator_data.get(code)
            if val is None:
                continue
            total_weight += weight
            if direction == "above":
                breach = max(0.0, val - threshold)
                severity = float(np.clip(breach / threshold * 100.0, 0, 100))
            else:
                breach = max(0.0, threshold - val)
                severity = float(np.clip(breach / max(abs(threshold), 1e-6) * 100.0, 0, 100))
            weighted_score += weight * severity
            if severity > 0:
                breach_details.append({
                    "indicator": code,
                    "value": round(val, 2),
                    "threshold": threshold,
                    "severity": round(severity, 2),
                })

        score = float(np.clip(weighted_score / total_weight * 1.0, 0, 100)) if total_weight > 0 else None
        if score is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable indicators"}

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "indicators_available": len(available),
                "indicators_breached": len(breach_details),
                "breach_details": breach_details,
                "indicator_values": {k: round(v, 2) for k, v in available.items()},
            },
        }
