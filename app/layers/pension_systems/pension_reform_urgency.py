"""Pension Reform Urgency module.

Composite reform urgency measure combining demographic pressure (old-age
dependency), fiscal strain (government debt), and coverage adequacy (social
transfers). When all three are simultaneously stressed, a triple-stress
amplifier is applied to capture non-linear reform urgency.

Base score = clip((dependency/3 + debt/3 + max(0, 30 - transfers)), 0, 100)
Triple-stress amplifier: if all three above thresholds, score *= 1.3 (capped at 100)

Thresholds: dependency > 20, debt_gdp > 60, transfers < 15

Sources: WDI SP.POP.DPND.OL (old-age dependency ratio),
         WDI GC.DOD.TOTL.GD.ZS (central government debt % of GDP),
         WDI GC.XPN.TRFT.ZS (social transfers % of expense)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_THRESHOLD_DEPENDENCY = 20.0
_THRESHOLD_DEBT = 60.0
_THRESHOLD_TRANSFERS = 15.0
_TRIPLE_STRESS_MULTIPLIER = 1.3


class PensionReformUrgency(LayerBase):
    layer_id = "lPS"
    name = "Pension Reform Urgency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        dependency_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SP.POP.DPND.OL'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        debt_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.DOD.TOTL.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        transfer_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TRFT.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not dependency_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no dependency ratio data"}
        if not debt_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no debt data"}

        dep_vals = [float(r["value"]) for r in dependency_rows if r["value"] is not None]
        debt_vals = [float(r["value"]) for r in debt_rows if r["value"] is not None]
        transfer_vals = [float(r["value"]) for r in transfer_rows if r["value"] is not None]

        if not dep_vals or not debt_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid data"}

        dependency = float(np.mean(dep_vals))
        debt_gdp = float(np.mean(debt_vals))
        transfers = float(np.mean(transfer_vals)) if transfer_vals else 5.0

        demographic_stress = dependency / 3.0
        fiscal_stress = debt_gdp / 3.0
        coverage_stress = max(0.0, 30.0 - transfers)

        base_score = float(np.clip(demographic_stress + fiscal_stress + coverage_stress, 0, 100))

        # Triple-stress amplifier
        triple_stressed = (
            dependency > _THRESHOLD_DEPENDENCY
            and debt_gdp > _THRESHOLD_DEBT
            and transfers < _THRESHOLD_TRANSFERS
        )
        final_score = float(np.clip(
            base_score * _TRIPLE_STRESS_MULTIPLIER if triple_stressed else base_score,
            0, 100,
        ))

        return {
            "score": round(final_score, 1),
            "country": country,
            "old_age_dependency_ratio": round(dependency, 2),
            "debt_gdp_pct": round(debt_gdp, 2),
            "social_transfers_pct_expense": round(transfers, 2),
            "demographic_stress_component": round(demographic_stress, 2),
            "fiscal_stress_component": round(fiscal_stress, 2),
            "coverage_stress_component": round(coverage_stress, 2),
            "base_score": round(base_score, 1),
            "triple_stress_amplified": triple_stressed,
            "thresholds": {
                "dependency": _THRESHOLD_DEPENDENCY,
                "debt_gdp": _THRESHOLD_DEBT,
                "transfers": _THRESHOLD_TRANSFERS,
            },
            "interpretation": (
                "urgent pension reform required" if final_score > 75
                else "pension reform highly advisable" if final_score > 50
                else "pension reform warranted" if final_score > 25
                else "pension system reform not urgent"
            ),
            "sources": ["WDI SP.POP.DPND.OL", "WDI GC.DOD.TOTL.GD.ZS", "WDI GC.XPN.TRFT.ZS"],
        }
