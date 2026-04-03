"""Sustainable Consumption: household consumption growth vs per capita CO2 emissions.

High household consumption growth accompanied by elevated per capita CO2 emissions
signals unsustainable consumption patterns. The score reflects the joint burden of
rapid consumption expansion and a carbon-intensive lifestyle.

Methodology:
    consumption_growth = mean(NE.CON.PRVT.KD.ZG) over available years
    co2_pc_latest = latest EN.ATM.CO2E.PC value

    Normalize each to [0, 50]:
        growth_score = clip(max(consumption_growth, 0) / 8 * 50, 0, 50)
            (8% annual growth as upper bound; negative growth -> 0)
        co2_score = clip(co2_pc_latest / 20 * 50, 0, 50)
            (20 tCO2/pc as upper bound for high emitters)

    score = growth_score + co2_score

References:
    Wiedmann, T. et al. (2020). "Scientists' warning on affluence." Nature
        Communications, 11, 3107.
    Jackson, T. (2009). Prosperity Without Growth. Earthscan, London.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_GROWTH_CAP = 8.0   # % annual consumption growth treated as upper bound
_CO2_CAP = 20.0     # tCO2 per capita upper bound


class SustainableConsumption(LayerBase):
    layer_id = "lSU"
    name = "Sustainable Consumption"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('NE.CON.PRVT.KD.ZG', 'EN.ATM.CO2E.PC')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 4:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient consumption/CO2 data"}

        series: dict[str, dict[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            yr = r["date"][:4] if isinstance(r["date"], str) else str(r["date"])
            series.setdefault(sid, {})[yr] = float(r["value"])

        cons_g = series.get("NE.CON.PRVT.KD.ZG", {})
        co2_pc = series.get("EN.ATM.CO2E.PC", {})

        if not cons_g and not co2_pc:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "no usable data series"}

        mean_growth = float(np.mean(list(cons_g.values()))) if cons_g else 0.0
        co2_latest = float(co2_pc[max(co2_pc.keys())]) if co2_pc else 0.0

        growth_score = float(np.clip(max(mean_growth, 0) / _GROWTH_CAP * 50, 0, 50))
        co2_score = float(np.clip(co2_latest / _CO2_CAP * 50, 0, 50))
        score = float(np.clip(growth_score + co2_score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "mean_consumption_growth_pct": round(mean_growth, 3),
            "co2_pc_tonnes_latest": round(co2_latest, 3),
            "growth_score": round(growth_score, 2),
            "co2_score": round(co2_score, 2),
            "n_consumption_years": len(cons_g),
            "n_co2_years": len(co2_pc),
        }
