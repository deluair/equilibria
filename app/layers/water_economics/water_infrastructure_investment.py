"""Water infrastructure investment: gross fixed capital formation relative to water access gaps.

Combines NE.GDI.FTOT.ZS (gross fixed capital formation as % of GDP) with
SH.H2O.BASW.ZS (basic water services access %) to assess whether investment
levels are sufficient to close the water infrastructure deficit.

Sources: World Bank WDI (NE.GDI.FTOT.ZS, SH.H2O.BASW.ZS)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class WaterInfrastructureInvestment(LayerBase):
    layer_id = "lWA"
    name = "Water Infrastructure Investment"

    async def compute(self, db, **kwargs) -> dict:
        gfcf_code = "NE.GDI.FTOT.ZS"
        gfcf_name = "gross fixed capital formation"
        gfcf_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gfcf_code, f"%{gfcf_name}%"),
        )

        water_code = "SH.H2O.BASW.ZS"
        water_name = "basic water services"
        water_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (water_code, f"%{water_name}%"),
        )

        gfcf_vals = [row["value"] for row in gfcf_rows if row["value"] is not None]
        water_vals = [row["value"] for row in water_rows if row["value"] is not None]

        if not gfcf_vals and not water_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No capital formation or water access data found",
            }

        gfcf_latest = float(gfcf_vals[0]) if gfcf_vals else None
        water_latest = float(water_vals[0]) if water_vals else None

        water_gap = max(0.0, 100.0 - water_latest) if water_latest is not None else 50.0

        # Investment adequacy: GFCF > 25% with small gap = adequate
        # Low GFCF + large gap = underinvestment = high risk
        invest_norm = min((gfcf_latest or 20.0) / 30.0, 1.0)
        gap_norm = water_gap / 100.0

        # Risk = gap not covered by investment
        risk = gap_norm * (1.0 - invest_norm * 0.5)
        score = round(min(100.0, risk * 100.0), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "gfcf_pct_gdp": round(gfcf_latest, 2) if gfcf_latest is not None else None,
                "water_access_pct": round(water_latest, 2) if water_latest is not None else None,
                "water_gap_pct": round(water_gap, 2),
                "investment_adequacy_norm": round(invest_norm, 3),
                "n_gfcf_obs": len(gfcf_vals),
                "n_water_obs": len(water_vals),
            },
        }
