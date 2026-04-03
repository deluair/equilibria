"""Trade capacity: logistics and trade facilitation quality.

Measures a country's capacity to trade efficiently via logistics performance
and customs efficiency. Poor logistics and high tariffs signal underdevelopment
and constrained trade integration.

Key references:
    Arvis, J.F. et al. (2023). Connecting to Compete: Trade Logistics in the
        Global Economy. World Bank LPI Report.
    Hummels, D. & Schaur, G. (2013). Time as a trade barrier. American Economic
        Review, 103(7), 2935-2959.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

LPI_FRONTIER = 4.0       # Max LPI score (5.0 scale), high-income benchmark ~4.0
TARIFF_SCALE = 5.0        # Multiplier: tariff % -> score units


class TradeCapacity(LayerBase):
    layer_id = "l4"
    name = "Trade Capacity"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Trade facilitation capacity via LPI or tariff fallback.

        Tries LP.LPI.OVRL.XQ (Logistics Performance Index, 1-5 scale) first.
        Score = max(0, 4 - lpi) * 25 (0 at LPI=4, 100 at LPI=0).
        Fallback: TM.TAX.MRCH.WM.AR.ZS (tariff rate, all products, weighted mean %).
        Score = clip(tariff * 5, 0, 100).

        Returns dict with score, LPI or tariff value, series used, and ranking.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Try LPI first
        lpi_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'LP.LPI.OVRL.XQ'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Fallback: tariff rates
        tariff_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'TM.TAX.MRCH.WM.AR.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not lpi_rows and not tariff_rows:
            return {"score": 50, "results": {"error": "no trade capacity data available"}}

        def build_dict(rows: list) -> dict[str, dict[str, float]]:
            d: dict[str, dict[str, float]] = {}
            for r in rows:
                d.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return d

        def get_latest(data: dict[str, dict[str, float]], iso: str) -> float | None:
            if iso not in data or not data[iso]:
                return None
            yr = max(data[iso].keys())
            return data[iso][yr]

        use_lpi = bool(lpi_rows)
        series_used = "LP.LPI.OVRL.XQ" if use_lpi else "TM.TAX.MRCH.WM.AR.ZS"
        primary_data = build_dict(lpi_rows if use_lpi else tariff_rows)

        # Global distribution
        latest_vals = []
        for iso_data in primary_data.values():
            if iso_data:
                yr = max(iso_data.keys())
                if iso_data[yr] is not None:
                    latest_vals.append((list(iso_data.keys())[-1], iso_data[yr]))

        all_vals = [v for _, v in latest_vals]
        global_median = float(np.median(all_vals)) if all_vals else None

        # Rankings
        iso_latest = {
            iso: data[max(data.keys())]
            for iso, data in primary_data.items()
            if data and data[max(data.keys())] is not None
        }
        if use_lpi:
            ranked = sorted(iso_latest.items(), key=lambda x: x[1], reverse=True)
        else:
            ranked = sorted(iso_latest.items(), key=lambda x: x[1])

        # Target country
        target_analysis = None
        score = 50.0

        if country_iso3:
            val = get_latest(primary_data, country_iso3)
            if val is not None:
                if use_lpi:
                    raw_score = max(0.0, LPI_FRONTIER - val) * 25.0
                else:
                    raw_score = val * TARIFF_SCALE
                score = float(np.clip(raw_score, 0, 100))

                # Rank among sample
                rank = sum(1 for v in iso_latest.values() if (v > val if use_lpi else v < val)) + 1
                n_countries = len(iso_latest)

                target_analysis = {
                    "series_used": series_used,
                    "value": val,
                    "lpi_frontier": LPI_FRONTIER if use_lpi else None,
                    "rank": rank,
                    "n_countries": n_countries,
                    "percentile_rank": float(rank / n_countries * 100),
                    "global_median": global_median,
                    "poor_logistics": use_lpi and val < 2.5,
                    "high_tariffs": not use_lpi and val > 10,
                }
        elif all_vals:
            if use_lpi:
                avg_gap = float(np.mean([max(0.0, LPI_FRONTIER - v) for v in all_vals]))
                score = float(np.clip(avg_gap * 25.0, 0, 100))
            else:
                score = float(np.clip(float(np.mean(all_vals)) * TARIFF_SCALE, 0, 100))

        return {
            "score": score,
            "results": {
                "series_used": series_used,
                "lpi_frontier": LPI_FRONTIER if use_lpi else None,
                "global_median": global_median,
                "n_countries": len(primary_data),
                "top_performers": ranked[:5],
                "bottom_performers": ranked[-5:],
                "target": target_analysis,
                "country_iso3": country_iso3,
            },
        }
