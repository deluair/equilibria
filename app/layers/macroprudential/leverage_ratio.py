"""System-wide leverage ratio.

Combines private sector credit and total domestic credit as a share of GDP
to measure aggregate financial leverage. High combined leverage signals
systemic fragility.

Score (0-100): clip((private_credit + total_credit) / 4 - 20, 0, 100).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class LeverageRatio(LayerBase):
    layer_id = "lMP"
    name = "System-Wide Leverage Ratio"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.series_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_code IN ('FS.AST.DOMS.GD.ZS', 'FS.AST.PRVT.GD.ZS')
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
                "error": "no leverage data available",
            }

        domestic: list[float] = []
        private: list[float] = []
        domestic_dates: list[str] = []
        private_dates: list[str] = []

        for r in rows:
            if r["series_code"] == "FS.AST.DOMS.GD.ZS":
                domestic.append(float(r["value"]))
                domestic_dates.append(r["date"])
            elif r["series_code"] == "FS.AST.PRVT.GD.ZS":
                private.append(float(r["value"]))
                private_dates.append(r["date"])

        total_credit = domestic[-1] if domestic else None
        private_credit = private[-1] if private else None

        if total_credit is None and private_credit is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no credit series with valid data",
            }

        combined = (private_credit or 0.0) + (total_credit or 0.0)
        score = float(np.clip(combined / 4.0 - 20.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "total_domestic_credit_gdp_pct": round(total_credit, 2) if total_credit is not None else None,
            "private_credit_gdp_pct": round(private_credit, 2) if private_credit is not None else None,
            "combined_leverage_pct": round(combined, 2),
            "latest_date": domestic_dates[-1] if domestic_dates else (private_dates[-1] if private_dates else None),
            "interpretation": self._interpret(combined, private_credit, total_credit),
        }

    @staticmethod
    def _interpret(combined: float, private: float | None, total: float | None) -> str:
        if combined > 180:
            return "extreme leverage: systemic fragility risk very high"
        if combined > 120:
            return "high leverage: elevated systemic risk"
        if combined > 80:
            return "moderate leverage: monitor credit expansion"
        return "low leverage: financial system not over-extended"
