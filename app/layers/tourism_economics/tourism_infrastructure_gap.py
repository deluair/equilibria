"""Tourism Infrastructure Gap module.

Proxies infrastructure readiness for tourism using:
  - Logistics Performance Index: Infrastructure quality (LP.LPI.INFR.XQ)
  - Electricity access (EG.ELC.ACCS.ZS) as a baseline services proxy

Low LPI infrastructure + low electricity access = high infrastructure gap = high score.

Score: 0 (excellent infrastructure) to 100 (severe gap).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class TourismInfrastructureGap(LayerBase):
    layer_id = "lTO"
    name = "Tourism Infrastructure Gap"

    async def compute(self, db, **kwargs) -> dict:
        lpi_code = "LP.LPI.INFR.XQ"
        elec_code = "EG.ELC.ACCS.ZS"

        lpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (lpi_code, "%infrastructure%"),
        )

        elec_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (elec_code, "%electricity access%"),
        )

        lpi_vals = [float(r["value"]) for r in lpi_rows if r["value"] is not None]
        elec_vals = [float(r["value"]) for r in elec_rows if r["value"] is not None]

        if not lpi_vals and not elec_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for LP.LPI.INFR.XQ or EG.ELC.ACCS.ZS",
            }

        components = []

        lpi_score_contrib = None
        if lpi_vals:
            latest_lpi = lpi_vals[0]
            # LPI ranges 1-5; 5 = best. Gap score: (5 - lpi) / 4 * 100
            lpi_score_contrib = float(np.clip((5.0 - latest_lpi) / 4.0 * 100, 0, 100))
            components.append(lpi_score_contrib)

        elec_score_contrib = None
        if elec_vals:
            latest_elec = elec_vals[0]
            # Electricity access 0-100%; gap = 100 - access
            elec_score_contrib = float(np.clip(100.0 - latest_elec, 0, 100))
            components.append(elec_score_contrib)

        score = float(np.mean(components))

        return {
            "score": round(score, 1),
            "lpi_infrastructure_score": round(lpi_vals[0], 3) if lpi_vals else None,
            "lpi_gap_score": round(lpi_score_contrib, 1) if lpi_score_contrib is not None else None,
            "electricity_access_pct": round(elec_vals[0], 2) if elec_vals else None,
            "electricity_gap_score": round(elec_score_contrib, 1) if elec_score_contrib is not None else None,
            "n_lpi_obs": len(lpi_vals),
            "n_elec_obs": len(elec_vals),
            "methodology": "avg(lpi_gap, electricity_gap); lpi_gap = (5 - lpi)/4*100",
        }
