"""Climate Resilience: adaptive capacity from governance, income, and disaster exposure.

Estimates a country's adaptive capacity to climate change by combining government
effectiveness (as a proxy for institutional adaptive capacity), real GDP per capita
(income-based adaptive resources), and meteorological disaster exposure. High
governance and income imply high resilience (low stress score).

Methodology:
    Normalize each dimension to [0, 1]:
        gov_score   = clip((GE.EST + 2.5) / 5.0, 0, 1)
            (WGI government effectiveness, range -2.5 to +2.5)
        income_score = clip(NY.GDP.PCAP.KD / 50000, 0, 1)
            ($50,000 per capita as upper bound)
        exposure_score = clip(EN.CLC.MDAT.ZS / 100, 0, 1)
            (population exposed to disasters; higher = more exposed)

    resilience_composite = (gov_score + income_score) / 2 * (1 - exposure_score * 0.3)
        (exposure penalizes resilience by up to 30%)

    score = 100 - clip(resilience_composite * 100, 0, 100)
        (high resilience -> low score; low resilience -> high score)

References:
    IPCC (2022). AR6 Working Group II: Impacts, Adaptation and Vulnerability.
    Brooks, N., Adger, W.N. & Kelly, P.M. (2005). "The determinants of vulnerability
        and adaptive capacity at the national level." Global Environmental Change,
        15(2), 151-163.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ClimateResilience(LayerBase):
    layer_id = "lSU"
    name = "Climate Resilience"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('GE.EST', 'NY.GDP.PCAP.KD', 'EN.CLC.MDAT.ZS')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 2:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient governance/income/disaster data"}

        series: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            series.setdefault(sid, {})[yr] = float(r["value"])

        def latest_val(sid: str) -> float | None:
            vals = series.get(sid, {})
            return float(vals[max(vals.keys())]) if vals else None

        ge = latest_val("GE.EST")
        gdp_pc = latest_val("NY.GDP.PCAP.KD")
        disaster = latest_val("EN.CLC.MDAT.ZS")

        gov_score = float(np.clip((ge + 2.5) / 5.0, 0, 1)) if ge is not None else 0.3
        income_score = float(np.clip(gdp_pc / 50000, 0, 1)) if gdp_pc is not None else 0.1
        exposure_score = float(np.clip(disaster / 100, 0, 1)) if disaster is not None else 0.5

        resilience = (gov_score + income_score) / 2 * (1 - exposure_score * 0.3)
        score = float(np.clip(100 - resilience * 100, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "governance_effectiveness": round(ge, 4) if ge is not None else None,
            "gdp_per_capita_usd": round(gdp_pc, 2) if gdp_pc is not None else None,
            "disaster_exposure_pct": round(disaster, 2) if disaster is not None else None,
            "gov_score": round(gov_score, 4),
            "income_score": round(income_score, 4),
            "exposure_score": round(exposure_score, 4),
            "resilience_composite": round(float(resilience), 4),
        }
