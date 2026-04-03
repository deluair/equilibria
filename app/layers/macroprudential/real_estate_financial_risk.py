"""Real Estate Financial Risk.

Mortgage credit exposure proxy: private credit expansion coinciding with rapid
urbanization signals real estate sector risk buildup in banking.

Score (0-100): weighted combination of private credit level and urban growth rate.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RealEstateFinancialRisk(LayerBase):
    layer_id = "lMP"
    name = "Real Estate Financial Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.series_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_code IN ('FS.AST.PRVT.GD.ZS', 'SP.URB.GROW')
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
                "error": "no private credit or urbanization data",
            }

        credit_vals: list[float] = []
        urban_vals: list[float] = []

        for r in rows:
            if r["series_code"] == "FS.AST.PRVT.GD.ZS":
                credit_vals.append(float(r["value"]))
            elif r["series_code"] == "SP.URB.GROW":
                urban_vals.append(float(r["value"]))

        private_credit = credit_vals[-1] if credit_vals else None
        urban_growth = float(np.mean(urban_vals[-5:])) if len(urban_vals) >= 3 else (urban_vals[-1] if urban_vals else None)

        if private_credit is None and urban_growth is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "both private credit and urbanization data missing",
            }

        # Score: credit component (high credit = risk) + urbanization component
        credit_score = float(np.clip((private_credit or 0.0) / 1.5, 0.0, 70.0))
        urban_score = float(np.clip((urban_growth or 0.0) * 5.0, 0.0, 30.0))
        score = float(np.clip(credit_score + urban_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "private_credit_gdp_pct": round(private_credit, 2) if private_credit is not None else None,
            "urban_growth_rate_pct": round(urban_growth, 4) if urban_growth is not None else None,
            "credit_score_component": round(credit_score, 2),
            "urban_score_component": round(urban_score, 2),
            "interpretation": self._interpret(private_credit, urban_growth),
        }

    @staticmethod
    def _interpret(credit: float | None, urban: float | None) -> str:
        parts = []
        if credit is not None:
            if credit > 80:
                parts.append(f"high private credit ({credit:.0f}% GDP)")
            elif credit > 50:
                parts.append(f"moderate private credit ({credit:.0f}% GDP)")
            else:
                parts.append(f"low private credit ({credit:.0f}% GDP)")
        if urban is not None:
            if urban > 3:
                parts.append(f"rapid urbanization ({urban:.1f}%/yr)")
            else:
                parts.append(f"moderate urbanization ({urban:.1f}%/yr)")
        return "; ".join(parts) if parts else "insufficient data"
