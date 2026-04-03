"""Macroprudential Stress Test Composite.

Weighted composite of four macroprudential risk dimensions:
  - NPL / asset quality stress
  - System-wide leverage
  - Liquidity coverage stress
  - Credit gap (countercyclical pressure)

All component scores are on 0-100. Final composite is a weighted average.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase
from app.layers.macroprudential.asset_quality_ratio import AssetQualityRatio
from app.layers.macroprudential.countercyclical_buffer import CountercyclicalBuffer
from app.layers.macroprudential.leverage_ratio import LeverageRatio
from app.layers.macroprudential.liquidity_coverage import LiquidityCoverage


class StressTestComposite(LayerBase):
    layer_id = "lMP"
    name = "Macroprudential Stress Test Composite"

    WEIGHTS = {
        "asset_quality": 0.30,
        "leverage": 0.25,
        "liquidity": 0.25,
        "credit_gap": 0.20,
    }

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Run component modules
        npl_result = await AssetQualityRatio().compute(db, **kwargs)
        leverage_result = await LeverageRatio().compute(db, **kwargs)
        liquidity_result = await LiquidityCoverage().compute(db, **kwargs)
        ccyb_result = await CountercyclicalBuffer().compute(db, **kwargs)

        components: dict[str, float | None] = {
            "asset_quality": npl_result.get("score"),
            "leverage": leverage_result.get("score"),
            "liquidity": liquidity_result.get("score"),
            "credit_gap": ccyb_result.get("score"),
        }

        available = {k: v for k, v in components.items() if v is not None}

        if not available:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no component data available for stress composite",
            }

        # Normalize weights to available components
        total_weight = sum(self.WEIGHTS[k] for k in available)
        composite = sum(
            v * self.WEIGHTS[k] / total_weight
            for k, v in available.items()
        )
        score = float(np.clip(composite, 0.0, 100.0))

        component_detail = {}
        for k in ["asset_quality", "leverage", "liquidity", "credit_gap"]:
            component_detail[k] = {
                "score": round(components[k], 2) if components[k] is not None else None,
                "weight": self.WEIGHTS[k],
                "included": components[k] is not None,
            }

        stress_flags = [k for k, v in components.items() if v is not None and v >= 50]

        return {
            "score": round(score, 2),
            "country": country,
            "components": component_detail,
            "components_available": len(available),
            "components_total": len(self.WEIGHTS),
            "effective_weight_coverage_pct": round(total_weight * 100, 1),
            "stress_flags": stress_flags,
            "stress_flag_count": len(stress_flags),
            "interpretation": self._interpret(score, stress_flags),
            "sub_results": {
                "asset_quality": {
                    "npl_ratio_pct": npl_result.get("npl_ratio_pct"),
                    "series_used": npl_result.get("series_used"),
                },
                "leverage": {
                    "combined_leverage_pct": leverage_result.get("combined_leverage_pct"),
                },
                "liquidity": {
                    "reserves_import_months": liquidity_result.get("reserves_import_months"),
                    "liquidity_stressed": liquidity_result.get("liquidity_stressed"),
                },
                "credit_gap": {
                    "credit_gap_pp": ccyb_result.get("credit_gap_pp"),
                    "buffer_activation_signal": ccyb_result.get("buffer_activation_signal"),
                },
            },
        }

    @staticmethod
    def _interpret(score: float, flags: list[str]) -> str:
        flag_str = ", ".join(flags) if flags else "none"
        if score >= 75:
            return f"macroprudential crisis conditions: stress flags [{flag_str}]"
        if score >= 50:
            return f"elevated macroprudential stress: flags [{flag_str}]"
        if score >= 25:
            return f"watch conditions: modest stress in [{flag_str}]" if flags else "system within bounds"
        return "macroprudential system stable"
