"""Climate-smart agriculture finance: resilience investment in irrigation and savings.

Methodology
-----------
**Climate-smart finance capacity** proxied from:
    - AG.LND.IRIG.AG.ZS: Agricultural land that is irrigated (% of total agricultural land).
      Irrigation is the primary climate adaptation technology; its coverage signals
      existing resilience investment.
    - NY.ADJ.SVNG.GN.ZS: Adjusted net savings (% of GNI), also called genuine savings.
      High adjusted savings indicate that the economy retains enough surplus to invest
      in climate-smart agricultural practices (irrigation, resilient seeds, soil health).

    resilience_index = 0.5 * irrigation_norm + 0.5 * savings_norm
        where:
            irrigation_norm = min(1, irrigation_pct / 30)   (30% benchmark = well-irrigated)
            savings_norm    = min(1, max(0, savings_pct / 20)) (20% GNI = high saving economy)

    score = (1 - resilience_index) * 100

Score (0-100): higher = weaker climate-smart finance capacity (more stress).

Sources: World Bank WDI (AG.LND.IRIG.AG.ZS, NY.ADJ.SVNG.GN.ZS)
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase

_SQL = """
    SELECT value FROM data_points
    WHERE series_id = (
        SELECT id FROM data_series
        WHERE indicator_code = ? OR name LIKE ?
    )
    ORDER BY date DESC LIMIT 15
"""

_IRRIGATION_BENCHMARK = 30.0  # % agricultural land irrigated
_SAVINGS_BENCHMARK = 20.0      # % GNI adjusted net savings


class ClimateSmartFinance(LayerBase):
    layer_id = "lAF"
    name = "Climate-Smart Finance"

    async def compute(self, db, **kwargs) -> dict:
        code_irr, name_irr = "AG.LND.IRIG.AG.ZS", "%agricultural land that is irrigated%"
        code_sav, name_sav = "NY.ADJ.SVNG.GN.ZS", "%adjusted net savings%"

        rows_irr = await db.fetch_all(_SQL, (code_irr, name_irr))
        rows_sav = await db.fetch_all(_SQL, (code_sav, name_sav))

        if not rows_irr and not rows_sav:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no irrigation or adjusted savings data"}

        irr_vals = [float(r["value"]) for r in rows_irr if r["value"] is not None]
        sav_vals = [float(r["value"]) for r in rows_sav if r["value"] is not None]

        irrigation = statistics.mean(irr_vals[:3]) if irr_vals else None
        savings = statistics.mean(sav_vals[:3]) if sav_vals else None

        metrics: dict = {
            "irrigated_land_pct": round(irrigation, 2) if irrigation is not None else None,
            "adjusted_net_savings_pct_gni": round(savings, 2) if savings is not None else None,
        }

        if irrigation is None and savings is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable data", "metrics": metrics}

        irr_norm = min(1.0, irrigation / _IRRIGATION_BENCHMARK) if irrigation is not None else 0.5
        sav_norm = max(0.0, min(1.0, savings / _SAVINGS_BENCHMARK)) if savings is not None else 0.5

        resilience_index = 0.5 * irr_norm + 0.5 * sav_norm
        score = max(0.0, min(100.0, (1.0 - resilience_index) * 100.0))

        metrics["irrigation_norm"] = round(irr_norm, 4)
        metrics["savings_norm"] = round(sav_norm, 4)
        metrics["resilience_index"] = round(resilience_index, 4)

        return {
            "score": round(score, 2),
            "metrics": metrics,
        }
