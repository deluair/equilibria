"""Circular business models: R&D and patent activity in green technology proxy.

Uses R&D expenditure as a share of GDP (GB.XPD.RSDV.GD.ZS) and resident
patent applications (IP.PAT.RESD) as proxies for the pace of innovation
in circular economy business models, green technology, and sustainable
product design. Higher R&D and patent activity correlates with greater
adoption of product-as-a-service, leasing, and remanufacturing models.

References:
    Antikainen, M. & Valkokari, K. (2016). A framework for sustainable circular
        business model innovation. Technology Innovation Management Review, 6(7).
    World Bank WDI: GB.XPD.RSDV.GD.ZS, IP.PAT.RESD
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CircularBusinessModels(LayerBase):
    layer_id = "lCE"
    name = "Circular Business Models"

    RD_CODE = "GB.XPD.RSDV.GD.ZS"
    PATENT_CODE = "IP.PAT.RESD"

    async def compute(self, db, **kwargs) -> dict:
        rd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.RD_CODE, f"%{self.RD_CODE}%"),
        )
        patent_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.PATENT_CODE, f"%{self.PATENT_CODE}%"),
        )

        if not rd_rows and not patent_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no R&D or patent data for circular business models",
            }

        rd_latest = None
        rd_trend = None
        if rd_rows:
            rd_vals = [r["value"] for r in rd_rows if r["value"] is not None]
            if rd_vals:
                rd_latest = float(rd_vals[0])
                if len(rd_vals) >= 3:
                    arr = np.array(rd_vals[:10], dtype=float)
                    rd_trend = float(np.polyfit(np.arange(len(arr)), arr, 1)[0])

        patent_latest = None
        patent_trend = None
        if patent_rows:
            patent_vals = [r["value"] for r in patent_rows if r["value"] is not None]
            if patent_vals:
                patent_latest = float(patent_vals[0])
                if len(patent_vals) >= 3:
                    arr = np.array(patent_vals[:10], dtype=float)
                    patent_trend = float(np.polyfit(np.arange(len(arr)), arr, 1)[0])

        if rd_latest is None and patent_latest is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "null R&D and patent values",
            }

        # R&D score: below 1% GDP = weak innovation; above 3% = strong
        if rd_latest is not None:
            if rd_latest >= 3.0:
                rd_score = 10.0
            elif rd_latest >= 2.0:
                rd_score = 25.0
            elif rd_latest >= 1.0:
                rd_score = 45.0
            else:
                rd_score = 70.0
        else:
            rd_score = 50.0

        # Patent score: growing patents = positive signal (lower stress)
        if patent_trend is not None:
            patent_score = max(10.0, 50.0 - patent_trend / max(patent_latest or 1, 1) * 500.0)
        else:
            patent_score = 50.0

        # Composite
        score = float(np.clip((rd_score + patent_score) / 2.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "rd_expenditure_pct_gdp": round(rd_latest, 3) if rd_latest is not None else None,
            "resident_patent_applications": round(patent_latest, 0) if patent_latest is not None else None,
            "rd_trend_slope_pp_yr": round(rd_trend, 4) if rd_trend is not None else None,
            "patent_trend_slope_yr": round(patent_trend, 1) if patent_trend is not None else None,
            "rd_component_score": round(rd_score, 2),
            "patent_component_score": round(patent_score, 2),
        }
