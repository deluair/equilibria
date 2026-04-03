"""Network Monopoly module.

Assesses monopoly risk in network industries (utilities, infrastructure) using
electricity access as a proxy for utility service reach and quality.

Rationale:
- Low electricity access despite moderate or high income indicates either
  under-investment by a monopoly utility or severe regulatory failure.
- The penalty is scaled by income: a poor country may have low access due
  to capital constraints (lower concern), whereas a middle/high-income
  country with low access signals monopoly/regulatory failure (higher concern).

Queries:
- EG.ELC.ACCS.ZS: access to electricity (% of population)
- NY.GNP.PCAP.CD: GNI per capita, Atlas method (USD) as income scaling

Score logic:
  access_gap = max(0, 100 - electricity_access)
  income_multiplier = log10(max(gni_pc, 100)) / log10(100000)  (0..1 scale)
  score = clip(access_gap * income_multiplier * 2, 0, 100)

A wealthy country with poor utility access receives a high stress score;
a low-income country with the same access gap receives a lower score.

Sources: WDI (EG.ELC.ACCS.ZS, NY.GNP.PCAP.CD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class NetworkMonopoly(LayerBase):
    layer_id = "lCO"
    name = "Network Monopoly"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        elec_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'EG.ELC.ACCS.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        gni_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GNP.PCAP.CD'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not elec_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no electricity access data"}

        def latest_value(rows) -> float | None:
            for r in rows:
                if r["value"] is not None:
                    try:
                        return float(r["value"])
                    except (TypeError, ValueError):
                        pass
            return None

        elec_access = latest_value(elec_rows)
        gni_pc = latest_value(gni_rows)

        if elec_access is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "missing electricity access"}

        access_gap = float(max(0, 100.0 - elec_access))

        # Income multiplier: low-income -> lower weight, high-income -> higher weight
        gni = gni_pc if gni_pc is not None else 5000.0  # neutral default
        income_multiplier = float(np.log10(max(gni, 100)) / np.log10(100_000))
        income_multiplier = float(np.clip(income_multiplier, 0.0, 1.0))

        score = float(np.clip(access_gap * income_multiplier * 2, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "electricity_access_pct": round(elec_access, 2),
            "access_gap_pct": round(access_gap, 2),
            "gni_per_capita_usd": round(gni, 1),
            "income_multiplier": round(income_multiplier, 4),
            "interpretation": (
                "adequate utility access" if score < 33
                else "partial utility gap" if score < 66
                else "severe network monopoly / regulatory failure"
            ),
            "reference": (
                "Joskow (2007): regulation of natural monopolies; "
                "IEA (2023): energy access"
            ),
        }
