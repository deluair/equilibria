"""Financial stress index module.

Combines inflation (FP.CPI.TOTL.ZG), real interest rate (FR.INR.RINR), and
private credit (FS.AST.PRVT.GD.ZS) into a financial stress composite. High
inflation erodes balance sheets; negative real rates distort credit allocation;
excessive credit amplifies shocks.

Score (0-100): stress rises with high inflation, negative real rates, or credit excess.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

CPI_CODE = "FP.CPI.TOTL.ZG"
CPI_NAME = "inflation consumer prices"
RINR_CODE = "FR.INR.RINR"
RINR_NAME = "real interest rate"
CREDIT_CODE = "FS.AST.PRVT.GD.ZS"
CREDIT_NAME = "domestic credit private sector"


class FinancialStressIndex(LayerBase):
    layer_id = "lFC"
    name = "Financial Stress Index"

    async def compute(self, db, **kwargs) -> dict:
        cpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (CPI_CODE, f"%{CPI_NAME}%"),
        )
        rinr_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (RINR_CODE, f"%{RINR_NAME}%"),
        )
        credit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (CREDIT_CODE, f"%{CREDIT_NAME}%"),
        )

        if not cpi_rows and not rinr_rows and not credit_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no financial stress data"}

        cpi_vals = [float(r["value"]) for r in cpi_rows if r["value"] is not None]
        rinr_vals = [float(r["value"]) for r in rinr_rows if r["value"] is not None]
        credit_vals = [float(r["value"]) for r in credit_rows if r["value"] is not None]

        cpi_latest = cpi_vals[0] if cpi_vals else None
        rinr_latest = rinr_vals[0] if rinr_vals else None
        credit_latest = credit_vals[0] if credit_vals else None

        # Inflation: >10% is stress, >20% is severe, <2% is benign
        cpi_score = 20.0
        if cpi_latest is not None:
            cpi_score = float(np.clip((cpi_latest - 2.0) * 5.0, 0, 100))

        # Real interest rate: negative (financial repression) or very high (debt stress)
        # Both extremes signal stress
        rinr_score = 20.0
        if rinr_latest is not None:
            # Negative real rates: financial repression / misallocation
            neg_stress = float(np.clip(-rinr_latest * 5.0, 0, 60))
            # Very high real rates: debt servicing stress (>10%)
            pos_stress = float(np.clip((rinr_latest - 8.0) * 5.0, 0, 60))
            rinr_score = max(neg_stress, pos_stress)

        # Credit: >100% GDP is stress territory
        credit_score = 20.0
        if credit_latest is not None:
            credit_score = float(np.clip((credit_latest - 40.0) * 1.2, 0, 60))

        components_available = sum(v is not None for v in [cpi_latest, rinr_latest, credit_latest])
        if components_available == 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all indicators missing"}

        weights = []
        raw = 0.0
        if cpi_latest is not None:
            weights.append(0.40)
            raw += 0.40 * cpi_score
        if rinr_latest is not None:
            weights.append(0.30)
            raw += 0.30 * rinr_score
        if credit_latest is not None:
            weights.append(0.30)
            raw += 0.30 * credit_score

        total_w = sum(weights)
        score = float(np.clip(raw / total_w, 0, 100)) if total_w > 0 else None
        if score is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "weight sum zero"}

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "inflation_pct": round(cpi_latest, 2) if cpi_latest is not None else None,
                "real_interest_rate_pct": round(rinr_latest, 2) if rinr_latest is not None else None,
                "private_credit_gdp_pct": round(credit_latest, 2) if credit_latest is not None else None,
                "inflation_score": round(cpi_score, 2),
                "real_rate_score": round(rinr_score, 2),
                "credit_score": round(credit_score, 2),
                "components_available": components_available,
            },
        }
