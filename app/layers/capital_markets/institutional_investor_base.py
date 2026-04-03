"""Institutional investor base analysis.

Pension fund assets and insurance company assets as a share of GDP indicate
the depth of long-term, patient capital available to domestic capital markets.
Institutional investors stabilise markets, lengthen investment horizons, and
absorb new equity/bond issuances.

Proxied by: life insurance premium volume (FI.RES.TOTL.CD as partial proxy),
pension contributions (SL.EMP.SMGT.ZS not ideal -- use available WDI insurance
series: IS.INS.PREM.CD and insurance penetration FS.AST.PRVT.GD.ZS).

Score (0-100): low institutional base = underdeveloped capital markets.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class InstitutionalInvestorBase(LayerBase):
    layer_id = "lCK"
    name = "Institutional Investor Base"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('FS.AST.PRVT.GD.ZS', 'FI.RES.TOTL.CD', 'IS.INS.PREM.CD')
              AND ds.country_iso3 = ?
              AND dp.date >= date('now', ?)
            ORDER BY ds.indicator_code, dp.date
            """,
            (country, f"-{lookback} years"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no institutional investor data",
            }

        by_code: dict[str, list[float]] = {}
        for r in rows:
            by_code.setdefault(r["indicator_code"], []).append(float(r["value"]))

        # Private credit/GDP as proxy for institutional depth
        if "FS.AST.PRVT.GD.ZS" in by_code:
            vals = np.array(by_code["FS.AST.PRVT.GD.ZS"])
            indicator_used = "FS.AST.PRVT.GD.ZS"
            label = "private_credit_pct_gdp"
        elif "FI.RES.TOTL.CD" in by_code:
            vals = np.array(by_code["FI.RES.TOTL.CD"])
            indicator_used = "FI.RES.TOTL.CD"
            label = "international_reserves_usd"
        elif "IS.INS.PREM.CD" in by_code:
            vals = np.array(by_code["IS.INS.PREM.CD"])
            indicator_used = "IS.INS.PREM.CD"
            label = "insurance_premiums_usd"
        else:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no institutional investor proxy series available",
            }

        latest = float(vals[-1])
        mean_val = float(np.mean(vals))

        # For private credit/GDP: 50% = moderate institutional depth
        # For USD-denominated series: normalise by latest / historical mean
        if "ZS" in indicator_used:
            score = float(np.clip(100.0 - latest, 0.0, 100.0))
        else:
            ratio = latest / mean_val if mean_val > 0 else 1.0
            score = float(np.clip(100.0 - ratio * 50.0, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "institutional_depth": {
                "metric": label,
                "indicator": indicator_used,
                "latest": round(latest, 2),
                "mean": round(mean_val, 2),
                "observations": len(vals),
            },
            "base_strength": (
                "weak" if score > 65
                else "developing" if score > 40
                else "moderate" if score > 20
                else "strong"
            ),
        }
