"""Arms trade balance: proxy for SIPRI arms trade flows via WDI trade indicators.

SIPRI Trend Indicator Values (TIVs) are the canonical arms trade measure but
require proprietary data. Here we proxy using WDI high-technology exports share
(TX.VAL.TECH.MF.ZS) as a correlate of defense industrial capacity, combined with
military expenditure share of GDP (MS.MIL.XPND.GD.ZS) to form a composite signal.

Countries with high tech export share and high military spend are typically
net arms exporters; low tech + high spend implies net importer dependency.

Score: net exporter profile -> STABLE, balanced -> WATCH, heavy importer
dependency with high spend -> STRESS, extreme import dependency -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ArmsTradeBalance(LayerBase):
    layer_id = "lDX"
    name = "Arms Trade Balance"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        tech_code = "TX.VAL.TECH.MF.ZS"
        mil_code = "MS.MIL.XPND.GD.ZS"

        tech_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (tech_code, "%high-technology exports%"),
        )
        mil_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (mil_code, "%military expenditure%GDP%"),
        )

        tech_vals = [r["value"] for r in tech_rows if r["value"] is not None]
        mil_vals = [r["value"] for r in mil_rows if r["value"] is not None]

        if not mil_vals and not tech_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for arms trade proxy indicators",
            }

        tech = tech_vals[0] if tech_vals else None
        mil = mil_vals[0] if mil_vals else 2.0  # global average fallback

        # Importer-dependency score: high mil spend + low tech exports = importer
        if tech is not None:
            importer_index = (mil / 2.0) * (1.0 - tech / 50.0)
        else:
            importer_index = mil / 2.0

        importer_index = max(0.0, importer_index)

        if importer_index < 0.5:
            score = 10.0
        elif importer_index < 1.0:
            score = 10.0 + (importer_index - 0.5) * 30.0
        elif importer_index < 2.0:
            score = 25.0 + (importer_index - 1.0) * 25.0
        elif importer_index < 3.0:
            score = 50.0 + (importer_index - 2.0) * 20.0
        else:
            score = min(100.0, 70.0 + (importer_index - 3.0) * 10.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "military_spending_gdp_pct": round(mil, 3),
                "high_tech_export_share_pct": round(tech, 3) if tech is not None else None,
                "importer_dependency_index": round(importer_index, 3),
                "n_obs_mil": len(mil_vals),
                "n_obs_tech": len(tech_vals),
                "profile": (
                    "net exporter" if importer_index < 0.5
                    else "balanced" if importer_index < 1.0
                    else "net importer" if importer_index < 2.0
                    else "heavy importer"
                ),
            },
        }
