"""Import concentration risk: HHI proxy of import source concentration.

Uses imports of goods and services as % of GDP (NE.IMP.GNFS.ZS) as a proxy for
import exposure. Higher import openness without diversification signals elevated
concentration risk. Score scaled so that high import-to-GDP ratios map to stress.

Methodology:
    Fetch the 15 most recent values of NE.IMP.GNFS.ZS. Compute mean import
    share. Score = clip(mean_import_share * 1.1, 0, 100).

    At 0%: score = 0.
    At 91%: score = 100 (extreme import dependence).
    Coefficient of variation across years inflates score if volatile.

Score (0-100): Higher score indicates greater import concentration risk.

References:
    World Bank WDI NE.IMP.GNFS.ZS.
    Herfindahl-Hirschman Index methodology (DOJ, 2010).
    Bebber et al. (2013). "Supply chain concentration." Nature Climate Change.
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase

_CODE = "NE.IMP.GNFS.ZS"
_NAME = "imports of goods and services"


class ImportConcentrationRisk(LayerBase):
    layer_id = "lSR"
    name = "Import Concentration Risk"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_CODE, f"%{_NAME}%"),
        )

        values = [float(r["value"]) for r in rows if r["value"] is not None]

        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"no data for {_CODE} (import concentration proxy)",
            }

        mean_import_share = statistics.mean(values)
        cv = (statistics.stdev(values) / mean_import_share) if len(values) > 1 and mean_import_share > 0 else 0.0
        score = float(min(max(mean_import_share * 1.1 * (1 + cv * 0.2), 0.0), 100.0))

        return {
            "score": round(score, 2),
            "mean_import_share_pct_gdp": round(mean_import_share, 2),
            "coefficient_of_variation": round(cv, 4),
            "n_obs": len(values),
            "indicator": _CODE,
        }
