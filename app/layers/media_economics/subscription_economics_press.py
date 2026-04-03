"""Subscription economics press: broadband penetration as digital news subscription proxy.

The shift from advertising-funded to subscription-funded journalism requires
a consumer base with reliable, affordable broadband access and sufficient
disposable income to pay for digital news. Fixed broadband penetration per
100 inhabitants is the strongest structural predictor of digital subscription
market depth. Low broadband with low income means the subscription model
cannot sustain journalism even where press freedom exists.

Score: high broadband + high income -> STABLE subscription market; moderate
broadband -> WATCH; low broadband with thin income base -> STRESS; minimal
broadband infrastructure -> CRISIS for subscription-funded press.
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase


class SubscriptionEconomicsPress(LayerBase):
    layer_id = "lMD"
    name = "Subscription Economics Press"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        bb_code = "IT.NET.BBND.P2"
        gdp_code = "NY.GDP.PCAP.PP.KD"

        bb_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (bb_code, "%broadband%"),
        )
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gdp_code, "%GDP per capita%"),
        )

        bb_vals = [r["value"] for r in bb_rows if r["value"] is not None]
        gdp_vals = [r["value"] for r in gdp_rows if r["value"] is not None]

        if not bb_vals and not gdp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for broadband IT.NET.BBND.P2 or GDP per capita",
            }

        bb_per100 = bb_vals[0] if bb_vals else None
        gdp_pc = gdp_vals[0] if gdp_vals else None

        # Broadband dimension: subscription capacity from infrastructure
        if bb_per100 is not None:
            if bb_per100 >= 40:
                bb_stress = 10.0
            elif bb_per100 >= 25:
                bb_stress = 15.0 + (40 - bb_per100) * 1.0
            elif bb_per100 >= 10:
                bb_stress = 30.0 + (25 - bb_per100) * 2.5
            elif bb_per100 >= 3:
                bb_stress = 67.5 + (10 - bb_per100) * 2.5
            else:
                bb_stress = min(100.0, 85.0 + (3 - bb_per100) * 5.0)
        else:
            bb_stress = 60.0  # unknown broadband -> moderate stress

        # Income dimension: willingness/ability to pay for digital subscriptions
        if gdp_pc is not None:
            gdp_log = math.log10(max(gdp_pc, 100))
            # $5k PPP = threshold where subscription becomes feasible
            # $30k PPP = mature subscription market
            gdp_stress = max(0.0, 50.0 - (gdp_log - math.log10(5000)) * 25.0)
            gdp_stress = min(80.0, max(5.0, gdp_stress))
        else:
            gdp_stress = 40.0

        # Weighted composite: broadband is primary driver
        score = round(0.65 * bb_stress + 0.35 * gdp_stress, 2)
        score = max(5.0, min(100.0, score))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "broadband_per_100": round(bb_per100, 2) if bb_per100 is not None else None,
                "gdp_per_capita_ppp": round(gdp_pc, 0) if gdp_pc is not None else None,
                "broadband_stress": round(bb_stress, 2),
                "income_stress": round(gdp_stress, 2),
                "n_obs_broadband": len(bb_vals),
                "n_obs_gdp": len(gdp_vals),
                "subscription_viable": (
                    bb_per100 is not None and bb_per100 >= 20 and
                    gdp_pc is not None and gdp_pc >= 8000
                ),
            },
        }
