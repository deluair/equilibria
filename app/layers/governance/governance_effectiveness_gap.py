"""Governance Effectiveness Gap module.

Measures the gap between a country's governance quality and what would be
expected given its income level.

Countries with high income but poor governance (rent-extracting states or
resource-cursed economies) have a large governance gap. Countries with good
governance relative to their income level (developmental states) have a
negative gap (no stress).

Method:
  1. Query GE.EST (Government Effectiveness, WGI) and
     NY.GDP.PCAP.KD (GDP per capita, constant 2015 USD).
  2. Expected governance = estimated via a simple log-linear relationship:
     expected_ge = a + b * log(gdp_pc)
     Since we have a single country, we use a cross-country benchmark.
     As a proxy, the expected GE for a given income level uses the
     empirically established relationship (World Bank):
       expected_ge ~ -3.5 + 0.6 * log10(gdp_pc)
     (approximation from cross-country evidence; GE ranges -2.5 to +2.5)
  3. gap = expected_ge - ge_actual
     Positive gap = underperforming relative to income = stress.
     score = clip(gap * 25, 0, 100).

Sources: World Bank WDI (GE.EST, NY.GDP.PCAP.KD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GovernanceEffectivenessGap(LayerBase):
    layer_id = "lGV"
    name = "Governance Effectiveness Gap"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('GE.EST', 'NY.GDP.PCAP.KD')
            ORDER BY ds.series_id, dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        series: dict[str, list[float]] = {}
        series_dates: dict[str, list[str]] = {}
        for r in rows:
            sid = r["series_id"]
            series.setdefault(sid, []).append(float(r["value"]))
            series_dates.setdefault(sid, []).append(r["date"])

        if "GE.EST" not in series:
            return {"score": None, "signal": "UNAVAILABLE", "error": "GE.EST not available"}

        ge_actual = float(series["GE.EST"][-1])

        gdp_pc = None
        expected_ge = None
        gap = None

        if "NY.GDP.PCAP.KD" in series:
            gdp_pc_vals = [v for v in series["NY.GDP.PCAP.KD"] if v > 0]
            if gdp_pc_vals:
                gdp_pc = float(gdp_pc_vals[-1])
                # Cross-country log-linear benchmark (World Bank empirical approximation)
                expected_ge = -3.5 + 0.6 * np.log10(max(gdp_pc, 1.0))
                expected_ge = float(np.clip(expected_ge, -2.5, 2.5))
                gap = expected_ge - ge_actual

        if gap is None:
            # No income data: score only on absolute GE level
            score = float(np.clip(50.0 - ge_actual * 20.0, 0.0, 100.0))
        else:
            # Positive gap = underperforming = stress
            score = float(np.clip(gap * 25.0, 0.0, 100.0))

        all_dates = [d for dates in series_dates.values() for d in dates]

        return {
            "score": round(score, 1),
            "country": country,
            "ge_actual": round(ge_actual, 4),
            "expected_ge": round(expected_ge, 4) if expected_ge is not None else None,
            "gap": round(gap, 4) if gap is not None else None,
            "gdp_per_capita_usd": round(gdp_pc, 2) if gdp_pc is not None else None,
            "underperforming": gap is not None and gap > 0,
            "indicators_used": list(series.keys()),
            "period": f"{min(all_dates)} to {max(all_dates)}",
            "note": "Gap = expected GE (from income) minus actual GE. Positive gap = governance deficit.",
        }
