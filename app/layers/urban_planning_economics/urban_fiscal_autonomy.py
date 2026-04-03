"""Urban Fiscal Autonomy module.

Measures the capacity of urban governments to raise and manage revenue autonomously.
High central tax dependence combined with poor government effectiveness signals
weak municipal fiscal autonomy.

Sources: WDI GC.TAX.TOTL.GD.ZS (tax revenue % of GDP),
         WGI GE.EST (government effectiveness, -2.5 to +2.5).
Score = clip((1 - ge_norm) * 60 + low_tax_capacity * 40, 0, 100).
Poor government effectiveness + low tax base = low fiscal autonomy risk.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class UrbanFiscalAutonomy(LayerBase):
    layer_id = "lUP"
    name = "Urban Fiscal Autonomy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        tax_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'GC.TAX.TOTL.GD.ZS'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        ge_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'GE.EST'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        if not tax_rows and not ge_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no fiscal data for urban fiscal autonomy"}

        score_components = []
        tax_val = None
        ge_val = None

        if ge_rows:
            # GE.EST: -2.5 to +2.5. Normalize to 0-1. Higher = more effective.
            ge_raw = float(ge_rows[0]["value"])
            ge_val = round(ge_raw, 3)
            ge_norm = float(np.clip((ge_raw + 2.5) / 5.0, 0, 1))
            # Poor effectiveness = lower fiscal autonomy (weighted 60%)
            score_components.append((1.0 - ge_norm) * 60.0)

        if tax_rows:
            # GC.TAX.TOTL.GD.ZS: tax revenue % of GDP. Low tax = low fiscal base.
            # Typical range 5-50%. Normalize: below 15% is low capacity.
            tax_raw = float(tax_rows[0]["value"])
            tax_val = round(tax_raw, 2)
            tax_norm = float(np.clip(tax_raw / 40.0, 0, 1))
            # Low tax capacity = low fiscal autonomy (weighted 40%)
            score_components.append((1.0 - tax_norm) * 40.0)

        if not score_components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable data for urban fiscal autonomy"}

        score = float(np.clip(sum(score_components), 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "tax_revenue_pct_gdp": tax_val,
            "government_effectiveness_wgi": ge_val,
            "interpretation": (
                "Low urban fiscal autonomy: municipalities dependent on central transfers"
                if score > 65
                else "Moderate fiscal autonomy constraints"
                if score > 35
                else "Relatively strong urban fiscal base and administrative capacity"
            ),
            "_sources": ["WDI:GC.TAX.TOTL.GD.ZS", "WGI:GE.EST"],
        }
