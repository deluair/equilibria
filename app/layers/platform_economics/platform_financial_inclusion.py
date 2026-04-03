"""Platform Financial Inclusion module.

Mobile subscription density and financial account ownership proxy access to
platform-based financial services (mobile money, digital payments).

Score: higher inclusion = lower score (better). Inverted so low inclusion = high risk.

Source: World Bank WDI (IT.CEL.SETS.P2, FX.OWN.TOTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PlatformFinancialInclusion(LayerBase):
    layer_id = "lPE"
    name = "Platform Financial Inclusion"

    async def compute(self, db, **kwargs) -> dict:
        code = "IT.CEL.SETS.P2"
        name = "mobile cellular subscriptions"
        mob_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        code2 = "FX.OWN.TOTL.ZS"
        name2 = "account ownership"
        acc_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = (SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) ORDER BY date DESC LIMIT 15",
            (code2, f"%{name2}%"),
        )

        if not mob_rows and not acc_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no mobile/account ownership data"}

        mob_vals = [float(r["value"]) for r in mob_rows if r["value"] is not None]
        acc_vals = [float(r["value"]) for r in acc_rows if r["value"] is not None]

        mob_mean = float(np.nanmean(mob_vals)) if mob_vals else None
        acc_mean = float(np.nanmean(acc_vals)) if acc_vals else None

        components, weights = [], []
        if mob_mean is not None:
            # Mobile subscriptions can exceed 100; cap at 150, normalize to 0-100
            components.append(float(np.clip(mob_mean / 1.5, 0, 100)))
            weights.append(0.5)
        if acc_mean is not None:
            components.append(float(np.clip(acc_mean, 0, 100)))
            weights.append(0.5)

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no usable values"}

        total_w = sum(weights)
        inclusion = sum(c * w for c, w in zip(components, weights)) / total_w
        # Invert: low inclusion = high exclusion risk
        score = float(np.clip(100 - inclusion, 0, 100))

        return {
            "score": round(score, 1),
            "mobile_subscriptions_per_100": round(mob_mean, 2) if mob_mean is not None else None,
            "account_ownership_pct": round(acc_mean, 2) if acc_mean is not None else None,
            "inclusion_index": round(inclusion, 1),
            "note": "Score inverted: higher score = lower inclusion = higher exclusion risk.",
            "_citation": "World Bank WDI: IT.CEL.SETS.P2, FX.OWN.TOTL.ZS",
        }
