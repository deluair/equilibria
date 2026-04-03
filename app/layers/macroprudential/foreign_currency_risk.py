"""Foreign Currency Risk (Currency Mismatch).

External debt denominated in foreign currency versus domestic reserve cushion.
High external debt combined with low reserve adequacy = currency mismatch stress.

Score (0-100): weighted blend of external debt ratio and reserve inadequacy.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ForeignCurrencyRisk(LayerBase):
    layer_id = "lMP"
    name = "Foreign Currency Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.series_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_code IN ('DT.DOD.DECT.GD.ZS', 'FI.RES.TOTL.MO')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.series_code, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no external debt or reserve data",
            }

        ext_debt_vals: list[float] = []
        reserves_vals: list[float] = []

        for r in rows:
            if r["series_code"] == "DT.DOD.DECT.GD.ZS":
                ext_debt_vals.append(float(r["value"]))
            elif r["series_code"] == "FI.RES.TOTL.MO":
                reserves_vals.append(float(r["value"]))

        ext_debt = ext_debt_vals[-1] if ext_debt_vals else None
        reserves_months = reserves_vals[-1] if reserves_vals else None

        if ext_debt is None and reserves_months is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for either indicator",
            }

        # External debt component: >60% GDP = high risk
        debt_score = float(np.clip((ext_debt or 0.0) / 60.0 * 60.0, 0.0, 60.0))
        # Reserve inadequacy component: <3 months = max stress
        reserve_gap = max(0.0, 6.0 - (reserves_months or 6.0))
        reserve_score = float(np.clip(reserve_gap / 6.0 * 40.0, 0.0, 40.0))

        score = float(np.clip(debt_score + reserve_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "external_debt_gdp_pct": round(ext_debt, 2) if ext_debt is not None else None,
            "reserves_import_months": round(reserves_months, 2) if reserves_months is not None else None,
            "debt_risk_component": round(debt_score, 2),
            "reserve_inadequacy_component": round(reserve_score, 2),
            "mismatch_stressed": score > 50,
            "interpretation": self._interpret(ext_debt, reserves_months),
        }

    @staticmethod
    def _interpret(debt: float | None, reserves: float | None) -> str:
        debt_str = f"external debt {debt:.0f}% GDP" if debt is not None else "external debt unknown"
        res_str = f"reserves {reserves:.1f} months" if reserves is not None else "reserves unknown"
        if debt is not None and debt > 60 and reserves is not None and reserves < 3:
            return f"severe currency mismatch stress: {debt_str}, {res_str}"
        if debt is not None and debt > 40:
            return f"elevated currency risk: {debt_str}, {res_str}"
        return f"manageable currency exposure: {debt_str}, {res_str}"
