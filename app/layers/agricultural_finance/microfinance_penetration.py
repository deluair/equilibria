"""Microfinance penetration: access to small-scale financial services in rural/ag sector.

Methodology
-----------
**Microfinance penetration** proxied from:
    - FX.OWN.TOTL.ZS: Account ownership at financial institution or mobile money
      (% of population 15+) -- proxy for financial inclusion breadth.
    - SL.AGR.EMPL.ZS: Employment in agriculture (% of total employment) -- proxy
      for the rural population most dependent on microfinance.

When financial account ownership is low and agriculture employs a large share of
workers, the implied microfinance gap is large. The interaction term captures
the structural demand-supply mismatch.

    inclusion_gap = max(0, ag_employment_share - account_ownership_pct)

A large gap indicates many agricultural workers lack basic financial accounts,
proxying poor microfinance penetration.

Score (0-100): higher = worse penetration (more stress).
    inclusion_gap > 50 ppt -> ~90
    inclusion_gap ~20 ppt -> ~50
    inclusion_gap < 0     -> ~10 (ag workers covered)

Sources: World Bank Global Findex (FX.OWN.TOTL.ZS), WDI (SL.AGR.EMPL.ZS)
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


class MicrofinancePenetration(LayerBase):
    layer_id = "lAF"
    name = "Microfinance Penetration"

    async def compute(self, db, **kwargs) -> dict:
        code_own, name_own = "FX.OWN.TOTL.ZS", "%account ownership at a financial institution%"
        code_ag, name_ag = "SL.AGR.EMPL.ZS", "%employment in agriculture%"

        rows_own = await db.fetch_all(_SQL, (code_own, name_own))
        rows_ag = await db.fetch_all(_SQL, (code_ag, name_ag))

        if not rows_own and not rows_ag:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no account ownership or agriculture employment data"}

        own_vals = [float(r["value"]) for r in rows_own if r["value"] is not None]
        ag_vals = [float(r["value"]) for r in rows_ag if r["value"] is not None]

        account_own = statistics.mean(own_vals[:3]) if own_vals else None
        ag_share = statistics.mean(ag_vals[:3]) if ag_vals else None

        metrics: dict = {
            "account_ownership_pct": round(account_own, 2) if account_own is not None else None,
            "ag_employment_share_pct": round(ag_share, 2) if ag_share is not None else None,
        }

        if account_own is None and ag_share is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable data", "metrics": metrics}

        # If only one indicator available, partial score
        if account_own is not None and ag_share is None:
            # Low account ownership alone signals poor microfinance
            score = max(0.0, min(100.0, 100.0 - account_own))
            metrics["inclusion_gap_ppt"] = None
        elif ag_share is not None and account_own is None:
            # High ag employment without account data -> assume gap proportional to ag share
            score = max(0.0, min(100.0, ag_share * 1.2))
            metrics["inclusion_gap_ppt"] = None
        else:
            inclusion_gap = max(0.0, ag_share - account_own)
            score = max(0.0, min(100.0, inclusion_gap * 1.5))
            metrics["inclusion_gap_ppt"] = round(inclusion_gap, 2)

        return {
            "score": round(score, 2),
            "metrics": metrics,
        }
