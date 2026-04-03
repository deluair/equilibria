"""Income Floor Adequacy module.

Evaluates whether the minimum income floor is adequate given the country's
income level. Combines social transfer spending (GC.XPN.TRFT.ZS) and GDP
per capita in constant USD (NY.GDP.PCAP.KD).

Low transfers for a country's income level = inadequate minimum floor.
Score = max(0, 15 - transfers_pct) * 4 + income_floor_gap, clipped to [0, 100].

Sources: WDI (GC.XPN.TRFT.ZS, NY.GDP.PCAP.KD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# GDP per capita benchmarks for floor expectation (constant 2015 USD)
_GDP_BENCHMARKS = [
    (30000, 20),   # high income: expect >= 20% transfers
    (10000, 15),   # upper-middle: expect >= 15%
    (3000, 10),    # lower-middle: expect >= 10%
    (0, 5),        # low income: expect >= 5%
]


def _expected_transfer_floor(gdp_pc: float) -> float:
    for threshold, floor in _GDP_BENCHMARKS:
        if gdp_pc >= threshold:
            return float(floor)
    return 5.0


class IncomeFloorAdequacy(LayerBase):
    layer_id = "lID"
    name = "Income Floor Adequacy"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        transfer_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'GC.XPN.TRFT.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not transfer_rows and not gdp_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        if transfer_rows:
            transfer_vals = np.array([float(r["value"]) for r in transfer_rows])
            transfers_pct = float(np.mean(transfer_vals[-3:]))
            transfer_period = f"{transfer_rows[0]['date']} to {transfer_rows[-1]['date']}"
        else:
            transfers_pct = 5.0
            transfer_period = None

        if gdp_rows:
            gdp_vals = np.array([float(r["value"]) for r in gdp_rows])
            gdp_pc = float(np.mean(gdp_vals[-3:]))
            gdp_period = f"{gdp_rows[0]['date']} to {gdp_rows[-1]['date']}"
        else:
            gdp_pc = 5000.0
            gdp_period = None

        expected_floor = _expected_transfer_floor(gdp_pc)
        income_floor_gap = float(np.clip(expected_floor - transfers_pct, 0, 20))
        transfer_shortfall = float(max(0.0, 15.0 - transfers_pct))

        score = float(np.clip(transfer_shortfall * 4 + income_floor_gap * 2, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "transfers_pct_of_expense": round(transfers_pct, 2),
            "transfer_period": transfer_period,
            "gdp_per_capita_usd": round(gdp_pc, 0),
            "gdp_period": gdp_period,
            "expected_transfer_floor_pct": round(expected_floor, 1),
            "income_floor_gap_pct": round(income_floor_gap, 2),
            "transfer_shortfall_from_15pct": round(transfer_shortfall, 2),
            "interpretation": (
                "gap = how far transfers fall below income-level-appropriate floor; "
                "higher = more inadequate minimum income protection"
            ),
        }
