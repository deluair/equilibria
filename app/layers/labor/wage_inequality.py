"""Wage inequality and labor income polarization.

Proxies wage polarization using two complementary WDI series:
  - SI.POV.GINI: Gini index (consumption/income inequality, 0-100)
  - SL.GDP.PCAP.EM.KD: GDP per person employed (constant 2017 USD) as a
    labor productivity / average wage proxy.

High Gini combined with low or stagnant labor productivity signals wage
polarization: the gains from growth are not flowing to median workers.

Scoring method (composite):
    gini_score = clip((gini - 30) * 2.0, 0, 100)
    productivity_gap: if productivity < 10,000 USD/worker -> high gap
    combined = 0.6 * gini_score + 0.4 * productivity_gap_score

    productivity_gap_score = clip((10000 - productivity) / 100, 0, 100)

Sources: WDI (SI.POV.GINI, SL.GDP.PCAP.EM.KD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

GINI_SERIES = "SI.POV.GINI"
PROD_SERIES = "SL.GDP.PCAP.EM.KD"


class WageInequality(LayerBase):
    layer_id = "l3"
    name = "Wage Inequality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT ds.series_id, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SI.POV.GINI', 'SL.GDP.PCAP.EM.KD')
              AND dp.value IS NOT NULL
            ORDER BY ds.series_id, dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no inequality/productivity data"}

        latest: dict[str, float] = {}
        latest_date: dict[str, str] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest:
                latest[sid] = float(r["value"])
                latest_date[sid] = r["date"]

        gini = latest.get(GINI_SERIES)
        productivity = latest.get(PROD_SERIES)

        # If only one series available, use what we have
        if gini is None and productivity is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "both Gini and productivity series missing"}

        components: dict[str, float] = {}

        if gini is not None:
            gini_score = float(np.clip((gini - 30.0) * 2.0, 0.0, 100.0))
            components["gini_score"] = round(gini_score, 2)
        else:
            gini_score = 50.0  # neutral if missing

        if productivity is not None:
            prod_gap_score = float(np.clip((10000.0 - productivity) / 100.0, 0.0, 100.0))
            components["productivity_gap_score"] = round(prod_gap_score, 2)
        else:
            prod_gap_score = 50.0  # neutral if missing

        # Weight: 0.6 Gini, 0.4 productivity gap
        if gini is not None and productivity is not None:
            score = 0.6 * gini_score + 0.4 * prod_gap_score
        elif gini is not None:
            score = gini_score
        else:
            score = prod_gap_score

        score = float(np.clip(score, 0.0, 100.0))

        result: dict = {
            "score": round(score, 2),
            "country": country,
            "components": components,
            "n_obs": len(rows),
            "note": (
                "score = 0.6*gini_score + 0.4*productivity_gap_score. "
                "High Gini + low productivity = wage polarization stress."
            ),
        }

        if gini is not None:
            result["gini_index"] = round(gini, 2)
            result["gini_date"] = latest_date.get(GINI_SERIES)
        if productivity is not None:
            result["gdp_per_worker_usd"] = round(productivity, 0)
            result["productivity_date"] = latest_date.get(PROD_SERIES)

        return result
