"""Mixed Use Development module.

Measures co-location of services and industry as a proxy for mixed-use urban form.
Balanced services/industry shares indicate mixed-use potential; extreme imbalance signals mono-functional zoning.

Sources: WDI NV.SRV.TOTL.ZS (services % of GDP), NV.IND.MANF.ZS (manufacturing % of GDP).
Score based on deviation from balanced co-location (40/20 ideal ratio).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MixedUseDevelopment(LayerBase):
    layer_id = "lUP"
    name = "Mixed Use Development"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        srv_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'NV.SRV.TOTL.ZS'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        mfg_rows = await db.fetch_all(
            """
            SELECT dp.value FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ? AND ds.series_id = 'NV.IND.MANF.ZS'
            ORDER BY dp.date DESC LIMIT 15
            """,
            (country,),
        )

        if not srv_rows or not mfg_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data for mixed use development"}

        srv_share = float(srv_rows[0]["value"])
        mfg_share = float(mfg_rows[0]["value"])

        # Mono-functional risk: services completely dominate (>75%) or manufacturing too low (<5%)
        # Score measures imbalance: higher = worse mixed-use conditions
        total = srv_share + mfg_share
        if total <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero economic activity shares"}

        srv_ratio = srv_share / total
        # Ideal mixed-use: services ~65-70% of combined. Penalize extremes.
        imbalance = abs(srv_ratio - 0.67) * 2  # 0-1 range
        score = float(np.clip(imbalance * 80, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "services_pct_gdp": round(srv_share, 2),
            "manufacturing_pct_gdp": round(mfg_share, 2),
            "services_to_combined_ratio": round(srv_ratio, 3),
            "interpretation": (
                "Strongly mono-functional economic structure: poor mixed-use conditions"
                if score > 60
                else "Moderate sectoral imbalance"
                if score > 30
                else "Balanced service-industry co-location"
            ),
            "_sources": ["WDI:NV.SRV.TOTL.ZS", "WDI:NV.IND.MANF.ZS"],
        }
