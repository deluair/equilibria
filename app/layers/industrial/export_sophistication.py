"""Export sophistication: high-technology exports as share of manufactured exports.

High-technology exports measure a country's capacity to produce and export
goods with high R&D intensity -- pharmaceuticals, aerospace, computers,
electronics, scientific instruments, and electrical machinery (OECD definition).
A high share signals strong industrial capability, productive specialization,
and durable comparative advantage (Hausmann & Hidalgo 2011).

Low high-tech export share is a structural vulnerability: countries locked
in low-technology manufactures face long-run terms-of-trade deterioration,
limited productivity spillovers, and shallow GVC integration.

Empirical benchmarks (World Bank WDI, 2015-2023 cross-section):
    < 5%:   very low sophistication (commodity/labor-intensive only)
    5-15%:  low-to-moderate sophistication
    15-30%: moderate (emerging industrial capacity)
    30-50%: high sophistication (OECD norm)
    > 50%:  very high (Singapore, South Korea, Switzerland)

Score formula (as specified):
    score = clip(max(0, 20 - hitech_pct) * 3.33, 0, 100)
    Uses the most recent available observation.

References:
    Hausmann, R. & Hidalgo, C. (2011). The network structure of economic output.
        JEG 16(3): 309-342.
    Lall, S. (2000). The technological structure of LDC exports. Oxford Dev Studies.
    World Bank WDI: TX.VAL.TECH.MF.ZS.

Indicator: TX.VAL.TECH.MF.ZS (High-technology exports, % of manufactured exports).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class ExportSophistication(LayerBase):
    layer_id = "l14"
    name = "Export Sophistication"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'TX.VAL.TECH.MF.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 2:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient high-tech export data",
            }

        dates = [r["date"] for r in rows]
        values = np.array([float(r["value"]) for r in rows], dtype=float)

        latest = float(values[-1])
        score = float(np.clip(max(0.0, 20.0 - latest) * 3.33, 0.0, 100.0))

        trend = None
        if len(values) >= 3:
            t = np.arange(len(values), dtype=float)
            slope, _, r_value, p_value, _ = linregress(t, values)
            trend = {
                "slope_pp_per_year": round(float(slope), 4),
                "r_squared": round(float(r_value ** 2), 4),
                "p_value": round(float(p_value), 4),
                "direction": "improving" if slope > 0 else "deteriorating",
            }

        return {
            "score": round(score, 2),
            "country": country,
            "latest_pct": round(latest, 2),
            "latest_year": dates[-1],
            "mean_pct": round(float(np.mean(values)), 2),
            "n_obs": len(values),
            "sophistication_tier": (
                "very low" if latest < 5
                else "low" if latest < 15
                else "moderate" if latest < 30
                else "high" if latest < 50
                else "very high"
            ),
            "trend": trend,
        }
