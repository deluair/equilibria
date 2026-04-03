"""Macroprudential Policy Index.

Combines credit growth (derived from FS.AST.PRVT.GD.ZS time series) and
bank stability (FB.AST.NPER.ZS) to assess whether macroprudential policy
is keeping pace with financial system risks.

Score (0-100): high credit growth + high NPL = policy failure / inadequate tools.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MacroprudentialPolicyIndex(LayerBase):
    layer_id = "lFR"
    name = "Macroprudential Policy Index"

    async def compute(self, db, **kwargs) -> dict:
        results = {}

        # Credit growth from private credit series
        credit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("FS.AST.PRVT.GD.ZS", "%private sector credit%"),
        )

        npl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("FB.AST.NPER.ZS", "%bank nonperforming loans%"),
        )

        credit_growth = None
        if credit_rows and len(credit_rows) >= 2:
            vals = [float(r["value"]) for r in credit_rows if r["value"] is not None]
            if len(vals) >= 2:
                # Most recent minus previous (desc order, so [0] is latest)
                credit_growth = vals[0] - vals[1]
                results["credit_growth_pp"] = credit_growth

        npl_latest = None
        if npl_rows:
            vals = [float(r["value"]) for r in npl_rows if r["value"] is not None]
            if vals:
                npl_latest = vals[0]
                results["npl_ratio_pct"] = npl_latest

        if not results:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no macroprudential policy proxy data found",
            }

        score_parts = []
        if credit_growth is not None:
            # Rapid credit growth (>5pp/yr) signals policy underreaction
            score_parts.append(float(np.clip(max(0.0, credit_growth) * 4.0, 0.0, 100.0)))
        if npl_latest is not None:
            # High NPL = policy already failed to prevent deterioration
            score_parts.append(float(np.clip(npl_latest * 6.67, 0.0, 100.0)))

        score = float(sum(score_parts) / len(score_parts))

        return {
            "score": round(score, 2),
            "credit_growth_pp": round(credit_growth, 4) if credit_growth is not None else None,
            "npl_ratio_pct": round(npl_latest, 2) if npl_latest is not None else None,
            "indicators_found": len(results),
            "interpretation": self._interpret(
                credit_growth if credit_growth is not None else 0.0,
                npl_latest if npl_latest is not None else 0.0,
            ),
        }

    @staticmethod
    def _interpret(credit_growth: float, npl: float) -> str:
        if credit_growth > 10 and npl > 10:
            return "macroprudential policy failing: rapid credit growth with high NPL"
        if credit_growth > 5 or npl > 10:
            return "policy under pressure: either excessive credit or elevated NPL"
        if credit_growth > 0 and npl > 5:
            return "moderate risk: credit expanding with elevated NPL"
        return "macroprudential conditions manageable: credit and NPL within bounds"
