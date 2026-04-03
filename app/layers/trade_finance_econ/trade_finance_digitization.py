"""Trade Finance Digitization module.

Digital readiness for trade finance transformation (e-LC, blockchain-based
bills of lading, digital factoring platforms). Internet penetration and
fixed broadband subscriptions proxy the infrastructure layer that enables
banks, corporates, and fintechs to adopt paperless trade finance.

Sources: WDI IT.NET.USER.ZS (internet users % population),
         WDI IT.NET.BBND.P2 (fixed broadband subscriptions per 100 people)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TradeFinanceDigitization(LayerBase):
    layer_id = "lTF"
    name = "Trade Finance Digitization"

    async def compute(self, db, **kwargs) -> dict:
        internet_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("IT.NET.USER.ZS", "%internet%users%"),
        )
        broadband_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            ("IT.NET.BBND.P2", "%broadband%subscriptions%"),
        )

        if not internet_rows and not broadband_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no digital infrastructure data"}

        internet_pct = float(internet_rows[0]["value"]) if internet_rows else 0.0
        broadband_per100 = float(broadband_rows[0]["value"]) if broadband_rows else 0.0

        # Internet: 0-100% population
        internet_norm = float(np.clip(internet_pct / 100.0, 0, 1))
        # Broadband: 0-50 per 100 is practical range
        broadband_norm = float(np.clip(broadband_per100 / 50.0, 0, 1))

        digital_readiness = (0.5 * internet_norm + 0.5 * broadband_norm) * 100

        # Low digitization = high friction/stress for digital trade finance
        score = float(np.clip(100 - digital_readiness, 0, 100))

        return {
            "score": round(score, 2),
            "internet_users_pct": round(internet_pct, 2) if internet_rows else None,
            "broadband_per_100": round(broadband_per100, 2) if broadband_rows else None,
            "digital_readiness_index": round(digital_readiness, 2),
            "interpretation": "Low internet and broadband penetration impedes trade finance digitization adoption",
        }
