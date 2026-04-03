"""SEEA compliance index: composite of environmental accounting adoption indicators.

The System of Environmental-Economic Accounting (SEEA, UN 2012/2021) provides a
statistical framework to integrate natural capital into national accounts. This module
constructs a proxy compliance index from the availability and non-zero reporting of
key SEEA-aligned World Bank indicators: adjusted net savings, natural resource
depletion, CO2 damage, PM2.5 damage, energy depletion, mineral depletion, and
forest depletion.

Compliance score = (number of indicators with valid data / total) * 100, adjusted
downward if key indicators (ANS, total depletion) are missing.

Score: full coverage -> low stress (20), no coverage -> high stress (85).

References:
    United Nations (2012). "System of Environmental-Economic Accounting: Central
        Framework." UN, New York.
    United Nations (2021). "System of Environmental-Economic Accounting—Ecosystem
        Accounting (SEEA EA)." UN, New York.
    World Bank WDI SEEA-aligned series.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

SEEA_INDICATORS = [
    ("NY.ADJ.SVNX.GN.ZS", "adjusted net savings"),
    ("NY.ADJ.DRES.GN.ZS", "natural resource depletion"),
    ("NY.ADJ.DCO2.GN.ZS", "CO2 damage"),
    ("NY.ADJ.DPEM.GN.ZS", "PM2.5 pollution damage"),
    ("NY.ADJ.DNGY.GN.ZS", "energy depletion"),
    ("NY.ADJ.DMIN.GN.ZS", "mineral depletion"),
    ("NY.ADJ.DFOR.GN.ZS", "net forest depletion"),
]

# Core indicators whose absence penalises the score more heavily
CORE_INDICATORS = {"NY.ADJ.SVNX.GN.ZS", "NY.ADJ.DRES.GN.ZS"}


class SeeaComplianceIndex(LayerBase):
    layer_id = "lEA"
    name = "SEEA Compliance Index"

    async def compute(self, db, **kwargs) -> dict:
        available = []
        missing = []

        for code, name in SEEA_INDICATORS:
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = ("
                "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (code, f"%{name}%"),
            )
            vals = [r["value"] for r in rows if r["value"] is not None]
            if vals:
                available.append(code)
            else:
                missing.append(code)

        n_total = len(SEEA_INDICATORS)
        n_available = len(available)
        base_coverage = n_available / n_total  # 0-1

        # Penalise missing core indicators
        core_missing = [c for c in CORE_INDICATORS if c in missing]
        penalty = len(core_missing) * 0.10

        compliance_score = max(0.0, base_coverage - penalty)

        # Invert for stress: full compliance -> low stress
        score = float(np.clip((1.0 - compliance_score) * 85.0 + 10.0, 10.0, 95.0))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "seea_indicators_total": n_total,
                "seea_indicators_available": n_available,
                "seea_indicators_missing": len(missing),
                "coverage_ratio": round(base_coverage, 4),
                "core_indicators_missing": core_missing,
                "compliance_score_0_1": round(compliance_score, 4),
                "available_codes": available,
                "missing_codes": missing,
            },
        }
