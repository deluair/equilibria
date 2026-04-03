"""Water sanitation economics: gap between basic water and sanitation access.

Combines SH.STA.BASS.ZS (basic sanitation services, % population) and
SH.H2O.BASW.ZS (basic water services, % population). The sanitation-water gap
reflects economic underinvestment; large gaps signal systemic infrastructure failure.

Sources: World Bank WDI (SH.STA.BASS.ZS, SH.H2O.BASW.ZS)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class WaterSanitationEconomics(LayerBase):
    layer_id = "lWA"
    name = "Water Sanitation Economics"

    async def compute(self, db, **kwargs) -> dict:
        san_code = "SH.STA.BASS.ZS"
        san_name = "basic sanitation services"
        san_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (san_code, f"%{san_name}%"),
        )

        water_code = "SH.H2O.BASW.ZS"
        water_name = "basic water services"
        water_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (water_code, f"%{water_name}%"),
        )

        san_vals = [row["value"] for row in san_rows if row["value"] is not None]
        water_vals = [row["value"] for row in water_rows if row["value"] is not None]

        if not san_vals and not water_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No sanitation or water access data found",
            }

        san_latest = float(san_vals[0]) if san_vals else None
        water_latest = float(water_vals[0]) if water_vals else None

        san_gap = max(0.0, 100.0 - san_latest) if san_latest is not None else None
        water_gap = max(0.0, 100.0 - water_latest) if water_latest is not None else None

        gap_values = [g for g in [san_gap, water_gap] if g is not None]
        avg_gap = sum(gap_values) / len(gap_values)

        # Score: average gap as 0-100 risk index
        score = round(min(100.0, avg_gap), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "sanitation_access_pct": round(san_latest, 2) if san_latest is not None else None,
                "water_access_pct": round(water_latest, 2) if water_latest is not None else None,
                "sanitation_gap_pct": round(san_gap, 2) if san_gap is not None else None,
                "water_gap_pct": round(water_gap, 2) if water_gap is not None else None,
                "avg_gap_pct": round(avg_gap, 2),
                "n_san_obs": len(san_vals),
                "n_water_obs": len(water_vals),
            },
        }
