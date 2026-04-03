"""Industrial symbiosis index: manufacturing and governance composite.

Combines manufacturing value added (NV.IND.MANF.ZS) with a regulatory
quality proxy (RQ.EST from World Governance Indicators) to estimate the
enabling environment for industrial symbiosis — where one firm's waste
becomes another's input. Higher manufacturing with better governance
creates stronger industrial ecology networks.

References:
    Chertow, M.R. (2000). Industrial symbiosis: Literature and taxonomy.
        Annual Review of Energy and the Environment, 25, 313-337.
    World Bank WDI: NV.IND.MANF.ZS, RQ.EST (Regulatory Quality)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class IndustrialSymbiosisIndex(LayerBase):
    layer_id = "lCE"
    name = "Industrial Symbiosis Index"

    MANF_CODE = "NV.IND.MANF.ZS"
    RQ_CODE = "RQ.EST"

    async def compute(self, db, **kwargs) -> dict:
        manf_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.MANF_CODE, f"%{self.MANF_CODE}%"),
        )
        rq_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self.RQ_CODE, f"%{self.RQ_CODE}%"),
        )

        if not manf_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no manufacturing value added data for industrial symbiosis",
            }

        manf_vals = [r["value"] for r in manf_rows if r["value"] is not None]
        if not manf_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "null manufacturing values",
            }

        manf_latest = float(manf_vals[0])

        rq_latest = None
        if rq_rows:
            rq_vals = [r["value"] for r in rq_rows if r["value"] is not None]
            if rq_vals:
                rq_latest = float(rq_vals[0])

        # Manufacturing share: 15-25% of GDP typical for industrial symbiosis hubs
        manf_score = float(np.clip((manf_latest - 10.0) / 20.0 * 50.0, 0.0, 50.0))

        # Regulatory quality: WGI RQ.EST ranges roughly -2.5 to +2.5
        if rq_latest is not None:
            # Normalize to 0-50: better governance = higher symbiosis potential
            rq_normalized = float(np.clip((rq_latest + 2.5) / 5.0 * 50.0, 0.0, 50.0))
        else:
            rq_normalized = 25.0  # neutral if unavailable

        # Composite symbiosis index (0-100, higher = stronger enabling environment)
        symbiosis_index = manf_score + rq_normalized

        # Score: low symbiosis index = higher stress for circular economy
        score = float(np.clip(100.0 - symbiosis_index, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "symbiosis_index": round(symbiosis_index, 2),
            "manufacturing_value_added_pct_gdp": round(manf_latest, 2),
            "regulatory_quality_est": round(rq_latest, 3) if rq_latest is not None else None,
            "manufacturing_component_score": round(manf_score, 2),
            "governance_component_score": round(rq_normalized, 2),
        }
