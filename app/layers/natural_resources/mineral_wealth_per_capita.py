"""Mineral wealth per capita: subsoil asset wealth per capita.

Uses World Bank Wealth Accounting data:
  NY.ADJ.DMIN.GN.ZS  - mineral depletion (% GNI) as a proxy flow variable,
  NY.GDP.PCAP.KD      - GDP per capita (constant USD) for income scaling.

Since direct subsoil asset stock data is not in standard WDI series, we proxy
mineral wealth per capita via capitalised mineral depletion:
  mineral_wealth_proxy = (mineral_depletion_pct_gni / 100) * gdp_per_capita * 20
  (20-year capitalisation rate approximation, consistent with World Bank CWON)

Score (higher = greater depletion relative to income, signalling drawdown risk):
  score = clip(mineral_depletion_pct * 10, 0, 100)

Sources: World Bank WDI (NY.ADJ.DMIN.GN.ZS, NY.GDP.PCAP.KD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_CAP_RATE = 20  # years, World Bank CWON approximation


class MineralWealthPerCapita(LayerBase):
    layer_id = "lNR"
    name = "Mineral Wealth Per Capita"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN ('NY.ADJ.DMIN.GN.ZS', 'NY.GDP.PCAP.KD')
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no mineral depletion or GDP per capita data",
            }

        latest: dict[str, tuple[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest and r["value"] is not None:
                latest[sid] = (r["date"][:4], float(r["value"]))

        depletion_data = latest.get("NY.ADJ.DMIN.GN.ZS")
        if depletion_data is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no mineral depletion data",
            }

        depletion_pct = depletion_data[1]
        latest_year = depletion_data[0]

        gdp_pc = None
        mineral_wealth_pc = None
        gdp_data = latest.get("NY.GDP.PCAP.KD")
        if gdp_data:
            gdp_pc = gdp_data[1]
            mineral_wealth_pc = (depletion_pct / 100.0) * gdp_pc * _CAP_RATE

        score = float(np.clip(depletion_pct * 10.0, 0, 100))

        depletion_level = (
            "negligible" if depletion_pct < 0.5
            else "low" if depletion_pct < 2.0
            else "moderate" if depletion_pct < 5.0
            else "high"
        )

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "latest_year": latest_year,
                "mineral_depletion_pct_gni": round(depletion_pct, 4),
                "gdp_per_capita_usd": round(gdp_pc, 2) if gdp_pc is not None else None,
                "mineral_wealth_proxy_per_capita_usd": (
                    round(mineral_wealth_pc, 2) if mineral_wealth_pc is not None else None
                ),
                "capitalisation_years": _CAP_RATE,
                "depletion_level": depletion_level,
            },
        }
