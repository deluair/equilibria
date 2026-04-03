"""Labor market efficiency: unemployment-labor force growth mismatch.

Persistent high unemployment alongside labor force growth signals a structural
mismatch: the economy is not creating jobs fast enough to absorb new entrants.
This proxy captures labor market efficiency loss without spell-level microdata.

Method:
    1. Fetch unemployment rate time series (SL.UEM.TOTL.ZS).
    2. Fetch total labor force (SL.TLF.TOTL.IN) and compute YoY growth rate.
    3. Compute mismatch score: high unemployment * high LF growth = worst case.

Scoring:
    base_score = unemployment_rate * 2.5  (40% rate -> score = 100)
    mismatch_amplifier = 1 + max(0, lf_growth_pct / 5)  (5% LF growth = 2x weight)
    score = clip(base_score * mismatch_amplifier, 0, 100)

    Example: UE=10%, LF growth=3% -> score = 25 * 1.6 = 40 (WATCH)
    Example: UE=20%, LF growth=5% -> score = 50 * 2.0 = 100 (CRISIS)

Sources: WDI (SL.UEM.TOTL.ZS, SL.TLF.TOTL.IN)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

UE_SERIES = "SL.UEM.TOTL.ZS"
LF_SERIES = "SL.TLF.TOTL.IN"


class LaborMarketEfficiency(LayerBase):
    layer_id = "l3"
    name = "Labor Market Efficiency"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SL.UEM.TOTL.ZS', 'SL.TLF.TOTL.IN')
              AND dp.value IS NOT NULL
            ORDER BY ds.series_id, dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no labor market data"}

        # Organize by series
        by_series: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            sid = r["series_id"]
            by_series.setdefault(sid, []).append((r["date"], float(r["value"])))

        if UE_SERIES not in by_series:
            return {"score": None, "signal": "UNAVAILABLE", "error": f"missing series {UE_SERIES}"}

        # Latest unemployment rate
        ue_sorted = sorted(by_series[UE_SERIES], key=lambda x: x[0], reverse=True)
        ue_rate = ue_sorted[0][1]
        ue_date = ue_sorted[0][0]

        # Labor force growth rate (YoY) from most recent two periods if available
        lf_growth_pct = None
        lf_date = None
        if LF_SERIES in by_series:
            lf_sorted = sorted(by_series[LF_SERIES], key=lambda x: x[0], reverse=True)
            if len(lf_sorted) >= 2:
                lf_curr = lf_sorted[0][1]
                lf_prev = lf_sorted[1][1]
                lf_date = lf_sorted[0][0]
                if lf_prev > 0:
                    lf_growth_pct = (lf_curr - lf_prev) / lf_prev * 100.0

        # Score
        base_score = ue_rate * 2.5
        if lf_growth_pct is not None and lf_growth_pct > 0:
            amplifier = 1.0 + max(0.0, lf_growth_pct / 5.0)
        else:
            amplifier = 1.0

        score = float(np.clip(base_score * amplifier, 0.0, 100.0))

        result: dict = {
            "score": round(score, 2),
            "country": country,
            "unemployment_rate_pct": round(ue_rate, 2),
            "unemployment_date": ue_date,
            "base_score": round(base_score, 2),
            "mismatch_amplifier": round(amplifier, 3),
            "n_obs": len(rows),
            "note": (
                "score = clip(ue_rate * 2.5 * (1 + max(0, lf_growth/5)), 0, 100). "
                "High unemployment + fast LF growth = efficiency loss."
            ),
        }

        if lf_growth_pct is not None:
            result["labor_force_growth_pct"] = round(lf_growth_pct, 3)
            result["labor_force_date"] = lf_date

        return result
