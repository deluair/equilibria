"""Remittance-financial inclusion linkage analysis.

High remittance flows (BX.TRF.PWKR.DT.GD.ZS) combined with low bank account
ownership (FX.OWN.TOTL.ZS) indicates informal channel dependency -- a financial
stress signal. When formal banking cannot capture remittance flows, the economy
loses multiplier effects and stability benefits.

Score (0-100): composite of remittance share and inverse of account ownership.
High remittance + low ownership pushes toward CRISIS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class RemittanceFinancialLinkage(LayerBase):
    layer_id = "l7"
    name = "Remittance Financial Linkage"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.fetch_all(
            """
            SELECT ds.indicator_code, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.indicator_code IN ('BX.TRF.PWKR.DT.GD.ZS', 'FX.OWN.TOTL.ZS')
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
                "error": "no remittance or account ownership data",
            }

        by_indicator: dict[str, list[float]] = {}
        for r in rows:
            by_indicator.setdefault(r["indicator_code"], []).append(float(r["value"]))

        remittance_vals = by_indicator.get("BX.TRF.PWKR.DT.GD.ZS", [])
        ownership_vals = by_indicator.get("FX.OWN.TOTL.ZS", [])

        if not remittance_vals and not ownership_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "neither remittance nor account ownership data available",
            }

        remittance_latest = float(np.mean(remittance_vals[-3:])) if remittance_vals else None
        ownership_latest = float(np.mean(ownership_vals[-3:])) if ownership_vals else None

        # Remittance component: high remittance = higher informal dependency risk
        # Normalize: >15% GDP = very high, 0% = none
        remittance_component = 50.0
        if remittance_latest is not None:
            remittance_component = float(np.clip(remittance_latest * 4.0, 0.0, 100.0))

        # Ownership component: low ownership = high informal dependency
        # Normalize: 0% ownership = 100 stress, 100% = 0 stress
        ownership_component = 50.0
        if ownership_latest is not None:
            ownership_component = float(np.clip(100.0 - ownership_latest, 0.0, 100.0))

        # Interaction: stress is highest when both are adverse
        if remittance_latest is not None and ownership_latest is not None:
            score = float(0.5 * remittance_component + 0.5 * ownership_component)
        elif remittance_latest is not None:
            score = remittance_component
        else:
            score = ownership_component

        informality_risk = (
            "high" if score > 60
            else "moderate" if score > 35
            else "low"
        )

        return {
            "score": round(score, 2),
            "country": country,
            "remittances_pct_gdp": round(remittance_latest, 3) if remittance_latest is not None else None,
            "bank_account_ownership_pct": round(ownership_latest, 2) if ownership_latest is not None else None,
            "informal_channel_dependency": informality_risk,
            "indicators": {
                "remittances": "BX.TRF.PWKR.DT.GD.ZS",
                "account_ownership": "FX.OWN.TOTL.ZS",
            },
        }
