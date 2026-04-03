"""AI investment concentration: ICT capital formation as AI investment proxy.

ICT capital formation (gross fixed capital formation in ICT equipment, software,
and communication equipment as a share of GDP) proxies the degree to which
economies are channeling investment into AI-enabling infrastructure. High ICT
capital formation concentrated in few sectors signals AI investment concentration
risk -- benefits accrue narrowly while adjustment costs spread broadly.

IMF (2023): AI investment is highly concentrated in North America and East Asia,
with developing economies receiving less than 5% of global AI investment.

Score: very low ICT investment -> CRISIS (excluded from AI capital stock),
high ICT investment -> STABLE (building AI-enabling infrastructure).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AIInvestmentConcentration(LayerBase):
    layer_id = "lAI"
    name = "AI Investment Concentration"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        ict_inv_code = "NE.GDI.FTOT.ZS"
        broadband_code = "IT.NET.BBND.P2"

        ict_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ict_inv_code, "%gross fixed capital formation%"),
        )
        bb_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (broadband_code, "%broadband%"),
        )

        ict_vals = [r["value"] for r in ict_rows if r["value"] is not None]
        bb_vals = [r["value"] for r in bb_rows if r["value"] is not None]

        if not ict_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for gross fixed capital formation NE.GDI.FTOT.ZS",
            }

        ict_share = ict_vals[0]
        broadband = bb_vals[0] if bb_vals else None

        # Score: lower investment = higher exclusion risk (higher stress)
        if ict_share >= 25:
            base = 10.0
        elif ict_share >= 20:
            base = 10.0 + (25.0 - ict_share) * 3.0
        elif ict_share >= 15:
            base = 25.0 + (20.0 - ict_share) * 4.0
        elif ict_share >= 10:
            base = 45.0 + (15.0 - ict_share) * 3.0
        else:
            base = min(95.0, 60.0 + (10.0 - ict_share) * 3.5)

        # Broadband penetration modifies investment utility
        if broadband is not None:
            if broadband >= 40:
                base = max(5.0, base - 10.0)
            elif broadband >= 20:
                base = max(5.0, base - 5.0)
            elif broadband < 5:
                base = min(100.0, base + 8.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "gross_fixed_capital_gdp_pct": round(ict_share, 2),
                "broadband_subscriptions_per_100": round(broadband, 2) if broadband is not None else None,
                "n_obs_investment": len(ict_vals),
                "n_obs_broadband": len(bb_vals),
                "investment_adequate": ict_share >= 20,
                "infrastructure_ready": broadband is not None and broadband >= 20,
            },
        }
