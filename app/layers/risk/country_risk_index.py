"""Country Risk Index module.

Composite country risk: political stability + governance + fiscal balance + external debt.
Queries WDI indicators:
  - PV.EST  : Political Stability and Absence of Violence (WGI, z-score)
  - GE.EST  : Government Effectiveness (WGI, z-score)
  - GC.BAL.CASH.GD.ZS : Cash surplus/deficit as % of GDP (negative = deficit)
  - DT.DOD.DECT.GD.ZS : External debt stocks as % of GNI

Each component is normalized to [0, 100] where 100 = highest risk.
Final score is a weighted average: governance 40%, fiscal 30%, debt 30%.

Sources: World Bank WDI (WGI, fiscal/debt data).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CountryRiskIndex(LayerBase):
    layer_id = "lRI"
    name = "Country Risk Index"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        indicators = {
            "PV.EST": None,
            "GE.EST": None,
            "GC.BAL.CASH.GD.ZS": None,
            "DT.DOD.DECT.GD.ZS": None,
        }

        for indicator in indicators:
            rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id = ?
                ORDER BY dp.date DESC
                LIMIT 5
                """,
                (country, indicator),
            )
            if rows:
                vals = [float(r["value"]) for r in rows if r["value"] is not None]
                if vals:
                    indicators[indicator] = float(np.mean(vals))

        present = {k: v for k, v in indicators.items() if v is not None}
        if len(present) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient indicators",
            }

        components = {}

        # Governance component: PV.EST and GE.EST are z-scores, typically -2.5 to 2.5
        # More negative = worse governance = higher risk
        gov_vals = []
        for key in ("PV.EST", "GE.EST"):
            if indicators[key] is not None:
                # Map [-2.5, 2.5] to [100, 0] (negative = high risk)
                risk = float(np.clip((2.5 - indicators[key]) / 5.0 * 100, 0, 100))
                gov_vals.append(risk)
                components[key] = round(risk, 2)
        gov_score = float(np.mean(gov_vals)) if gov_vals else None

        # Fiscal balance: deficit (negative values) -> higher risk
        # Range: typically -10% to +5%. Deficit > 5% = high risk.
        fiscal_score = None
        if indicators["GC.BAL.CASH.GD.ZS"] is not None:
            balance = indicators["GC.BAL.CASH.GD.ZS"]
            # Flip sign: deficit is negative balance
            fiscal_score = float(np.clip((-balance) / 10.0 * 100, 0, 100))
            components["GC.BAL.CASH.GD.ZS"] = round(fiscal_score, 2)

        # External debt: > 60% GNI = elevated risk, > 100% = very high
        debt_score = None
        if indicators["DT.DOD.DECT.GD.ZS"] is not None:
            debt = indicators["DT.DOD.DECT.GD.ZS"]
            debt_score = float(np.clip(debt / 100.0 * 60, 0, 100))
            components["DT.DOD.DECT.GD.ZS"] = round(debt_score, 2)

        # Weighted composite
        weights = []
        scores = []
        if gov_score is not None:
            weights.append(0.40)
            scores.append(gov_score)
        if fiscal_score is not None:
            weights.append(0.30)
            scores.append(fiscal_score)
        if debt_score is not None:
            weights.append(0.30)
            scores.append(debt_score)

        if not scores:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no scoreable components",
            }

        w_arr = np.array(weights)
        w_arr = w_arr / w_arr.sum()
        composite = float(np.dot(w_arr, scores))

        return {
            "score": round(composite, 1),
            "country": country,
            "components": components,
            "raw_indicators": {k: round(v, 4) for k, v in indicators.items() if v is not None},
        }
