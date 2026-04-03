"""Input finance access: ability to finance agricultural inputs (fertilizer, seed, etc.).

Methodology
-----------
**Input finance access** proxied from:
    - AG.YLD.CREL.KG: Cereal yield (kg per hectare) -- higher yield indicates
      farmers can afford and apply modern inputs (seeds, fertilizer, machinery).
    - FS.AST.PRVT.GD.ZS: Domestic credit to private sector (% of GDP) -- credit
      availability proxy for input financing.

Higher cereal yields alongside broader credit access indicate farmers can finance
inputs. Low yields in a credit-constrained environment signal an input finance gap.

    yield_score: normalised against a 5,000 kg/ha benchmark (global median ~3,900)
    credit_score: normalised against 50% of GDP as moderate

    input_finance_score = 0.6 * (1 - yield_norm) + 0.4 * (1 - credit_norm)

Score (0-100): higher = worse input finance access (more stress).

Sources: World Bank WDI (AG.YLD.CREL.KG, FS.AST.PRVT.GD.ZS)
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

_YIELD_BENCHMARK = 5000.0   # kg/ha -- global median ~3,900; 5,000 = moderate-high
_CREDIT_BENCHMARK = 50.0    # % of GDP -- moderate financial depth


class InputFinanceAccess(LayerBase):
    layer_id = "lAF"
    name = "Input Finance Access"

    async def compute(self, db, **kwargs) -> dict:
        code_yld, name_yld = "AG.YLD.CREL.KG", "%cereal yield%"
        code_cr, name_cr = "FS.AST.PRVT.GD.ZS", "%domestic credit to private sector%"

        rows_yld = await db.fetch_all(_SQL, (code_yld, name_yld))
        rows_cr = await db.fetch_all(_SQL, (code_cr, name_cr))

        if not rows_yld and not rows_cr:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no cereal yield or credit data"}

        yld_vals = [float(r["value"]) for r in rows_yld if r["value"] is not None]
        cr_vals = [float(r["value"]) for r in rows_cr if r["value"] is not None]

        yld = statistics.mean(yld_vals[:3]) if yld_vals else None
        credit = statistics.mean(cr_vals[:3]) if cr_vals else None

        metrics: dict = {
            "cereal_yield_kg_ha": round(yld, 1) if yld is not None else None,
            "credit_pct_gdp": round(credit, 2) if credit is not None else None,
        }

        if yld is None and credit is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable data", "metrics": metrics}

        yield_norm = min(1.0, yld / _YIELD_BENCHMARK) if yld is not None else 0.5
        credit_norm = min(1.0, credit / _CREDIT_BENCHMARK) if credit is not None else 0.5

        score = max(0.0, min(100.0,
            0.6 * (1.0 - yield_norm) * 100.0 + 0.4 * (1.0 - credit_norm) * 100.0
        ))

        metrics["yield_norm"] = round(yield_norm, 4)
        metrics["credit_norm"] = round(credit_norm, 4)

        return {
            "score": round(score, 2),
            "metrics": metrics,
        }
