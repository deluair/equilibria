"""Catastrophic health spending analysis.

Estimates the proportion of households facing catastrophic health expenditure
(CHE), defined as out-of-pocket health payments exceeding 10% of household
total expenditure or 25% of non-food expenditure (WHO/World Bank definition).

Uses World Bank poverty/health indicators as proxies where direct CHE survey
data is unavailable. High CHE incidence signals inadequate financial protection
in the health system.

Key references:
    Xu, K. et al. (2003). Household catastrophic health expenditure: a
        multicountry analysis. The Lancet, 362(9378), 111-117.
    World Bank (2020). Poverty and Shared Prosperity Report.
    WHO (2019). Global monitoring report on financial protection in health.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CatastrophicHealthSpending(LayerBase):
    layer_id = "lHF"
    name = "Catastrophic Health Spending"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate catastrophic health spending incidence.

        Uses OOP spending share as primary proxy (SH.XPD.OOPC.CH.ZS) and
        poverty headcount ratio (SI.POV.DDAY) as a complementary indicator.
        Countries with OOP > 25% of THE and high poverty rates are classified
        as high catastrophic spending risk.

        Returns dict with score, signal, and CHE risk metrics.
        """
        oop_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.OOPC.CH.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        poverty_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SI.POV.DDAY'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        if not oop_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No OOP health spending data in DB",
            }

        def _latest(rows) -> dict[str, float]:
            out: dict[str, float] = {}
            for row in rows:
                iso = row["country_iso3"]
                if iso not in out and row["value"] is not None:
                    out[iso] = float(row["value"])
            return out

        oop_data = _latest(oop_rows)
        poverty_data = _latest(poverty_rows)

        # CHE proxy: OOP > 25% of THE = elevated CHE risk
        # Combined: OOP > 25% AND poverty > 10% = high CHE incidence
        che_high: list[str] = []
        che_moderate: list[str] = []
        che_low: list[str] = []

        for iso, oop_val in oop_data.items():
            pov_val = poverty_data.get(iso, 0.0) or 0.0
            if oop_val > 40 and pov_val > 10:
                che_high.append(iso)
            elif oop_val > 25 or (oop_val > 15 and pov_val > 20):
                che_moderate.append(iso)
            else:
                che_low.append(iso)

        n = len(oop_data)
        if n == 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid OOP values after filtering",
            }

        # Score: weighted stress by CHE tier
        stress = (len(che_high) * 1.0 + len(che_moderate) * 0.5) / n
        score = float(np.clip(stress * 100, 0, 100))

        mean_oop = float(np.mean(list(oop_data.values())))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "n_countries": n,
                "mean_oop_pct_the": round(mean_oop, 2),
                "countries_high_che_risk": len(che_high),
                "countries_moderate_che_risk": len(che_moderate),
                "countries_low_che_risk": len(che_low),
                "pct_high_che_risk": round(100.0 * len(che_high) / n, 1),
                "pct_moderate_che_risk": round(100.0 * len(che_moderate) / n, 1),
                "che_threshold_oop_pct": 25.0,
                "has_poverty_data": len(poverty_data) > 0,
            },
        }
