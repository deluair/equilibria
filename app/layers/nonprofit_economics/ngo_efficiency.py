"""NGO efficiency: ODA and aid effectiveness indicators.

Official Development Assistance (ODA) flows through governments, multilaterals,
and NGOs. Effectiveness is proxied via net ODA received per capita alongside
aid absorption capacity. High ODA per capita with poor governance outcomes
signals NGO inefficiency; low ODA with strong institutions signals self-reliance.
WDI aid effectiveness indicators (DT.ODA.ODAT.PC.ZS, IQ.CPA.GNDR.XQ).

Score: captures the gap between aid received and institutional quality.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class NGOEfficiency(LayerBase):
    layer_id = "lNP"
    name = "NGO Efficiency"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        oda_code = "DT.ODA.ODAT.PC.ZS"
        gov_code = "IQ.CPA.GNDR.XQ"

        oda_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (oda_code, "%ODA received per capita%"),
        )
        gov_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gov_code, "%CPIA gender equality%"),
        )

        oda_vals = [r["value"] for r in oda_rows if r["value"] is not None]
        gov_vals = [r["value"] for r in gov_rows if r["value"] is not None]

        if not oda_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for ODA per capita",
            }

        oda_pc = oda_vals[0]
        trend = round(oda_vals[0] - oda_vals[-1], 3) if len(oda_vals) > 1 else None

        # Governance quality modifier (CPIA 1-6 scale, higher = better)
        gov_score = gov_vals[0] if gov_vals else 3.0
        gov_modifier = (4.0 - gov_score) * 5.0  # poor governance amplifies inefficiency

        # Base score from ODA dependency
        if oda_pc < 5:
            base = 10.0 + oda_pc * 2.0
        elif oda_pc < 25:
            base = 20.0 + (oda_pc - 5) * 1.25
        elif oda_pc < 75:
            base = 45.0 + (oda_pc - 25) * 0.5
        else:
            base = min(100.0, 70.0 + (oda_pc - 75) * 0.3)

        score = min(100.0, max(0.0, base + gov_modifier))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "oda_per_capita_usd": round(oda_pc, 2),
                "governance_quality_score": round(gov_score, 2),
                "trend_oda_change": trend,
                "n_obs_oda": len(oda_vals),
                "n_obs_gov": len(gov_vals),
            },
        }
