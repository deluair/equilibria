"""Out-of-pocket health spending burden.

Measures out-of-pocket (OOP) health expenditure as a share of total current
health expenditure. High OOP shares indicate inadequate prepayment mechanisms
(insurance or tax-funded systems), leaving households exposed to financial risk
from health shocks.

WHO/World Bank financial protection benchmarks:
    OOP < 15-20% of THE: good financial protection
    OOP 20-40% of THE: moderate risk
    OOP > 40% of THE: high catastrophic spending risk

Key references:
    Xu, K. et al. (2003). Household catastrophic health expenditure: a
        multicountry analysis. The Lancet, 362(9378), 111-117.
    WHO/World Bank (2017). Tracking universal health coverage: 2017 global
        monitoring report.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class OopHealthBurden(LayerBase):
    layer_id = "lHF"
    name = "OOP Health Burden"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Compute out-of-pocket health spending burden.

        Fetches OOP expenditure as % of current health expenditure (SH.XPD.OOPC.CH.ZS).
        Classifies countries by financial protection risk tier and computes
        cross-country distribution metrics.

        Returns dict with score, signal, and OOP burden metrics.
        """
        rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.OOPC.CH.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No OOP health expenditure data in DB",
            }

        latest: dict[str, float] = {}
        for row in rows:
            iso = row["country_iso3"]
            if iso not in latest and row["value"] is not None:
                latest[iso] = float(row["value"])

        values = list(latest.values())
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid OOP expenditure values",
            }

        # WHO financial protection tiers
        low_risk = [v for v in values if v < 20]
        moderate_risk = [v for v in values if 20 <= v < 40]
        high_risk = [v for v in values if v >= 40]

        mean_oop = float(np.mean(values))
        median_oop = float(np.median(values))

        # Score: weighted by severity of OOP burden
        # Countries >40% OOP contribute most to stress score
        stress_component = (len(high_risk) * 1.0 + len(moderate_risk) * 0.5) / len(values)
        score = float(np.clip(stress_component * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "n_countries": len(values),
                "mean_oop_pct_the": round(mean_oop, 2),
                "median_oop_pct_the": round(median_oop, 2),
                "countries_low_risk_oop_lt20pct": len(low_risk),
                "countries_moderate_risk_oop_20_40pct": len(moderate_risk),
                "countries_high_risk_oop_gt40pct": len(high_risk),
                "pct_high_risk": round(100.0 * len(high_risk) / len(values), 1),
            },
        }
