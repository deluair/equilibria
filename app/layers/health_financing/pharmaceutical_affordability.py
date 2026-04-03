"""Pharmaceutical affordability: essential medicines price burden.

Assesses the affordability of essential medicines relative to household income.
High pharmaceutical costs represent a key driver of out-of-pocket health spending
and catastrophic expenditure, especially in low- and middle-income countries.

Uses health expenditure per capita and OOP spending shares as proxies for
medicine affordability where direct pharmaceutical price indices are unavailable.
Also incorporates generic medicine adoption indicators where available.

Key references:
    Cameron, A. et al. (2009). Medicine prices, availability, and affordability
        in 36 developing and middle-income countries: a secondary analysis.
        The Lancet, 373(9659), 240-249.
    Bigdeli, M. et al. (2014). Access to medicines from a health system
        perspective. Health Policy and Planning, 29(7), 764-775.
    WHO/HAI (2008). Measuring medicine prices, availability, affordability
        and price components. 2nd edition.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PharmaceuticalAffordability(LayerBase):
    layer_id = "lHF"
    name = "Pharmaceutical Affordability"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate pharmaceutical affordability from spending and income data.

        Uses health expenditure per capita (SH.XPD.CHEX.PC.CD), OOP share
        (SH.XPD.OOPC.CH.ZS), and GDP per capita (NY.GDP.PCAP.KD) to construct
        a pharmaceutical affordability proxy. High OOP relative to income
        signals unaffordable medicines.

        Returns dict with score, signal, and affordability proxy metrics.
        """
        hepc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.CHEX.PC.CD'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

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

        gdppc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        if not hepc_rows or not oop_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No health expenditure per capita or OOP data in DB",
            }

        def _latest(rows) -> dict[str, float]:
            out: dict[str, float] = {}
            for row in rows:
                iso = row["country_iso3"]
                if iso not in out and row["value"] is not None:
                    out[iso] = float(row["value"])
            return out

        hepc_data = _latest(hepc_rows)
        oop_data = _latest(oop_rows)
        gdppc_data = _latest(gdppc_rows)

        # Affordability proxy: OOP spending per capita as % of GDP per capita
        # Assumes ~40-60% of OOP is on pharmaceuticals (Cameron et al. 2009)
        pharma_oop_fraction = 0.5  # WHO estimate: ~50% of OOP is medicines

        affordability_ratios: list[float] = []
        for iso in set(hepc_data.keys()) & set(oop_data.keys()) & set(gdppc_data.keys()):
            hepc = hepc_data[iso]
            oop_share = oop_data[iso]
            gdppc = gdppc_data[iso]
            if hepc > 0 and oop_share > 0 and gdppc > 0:
                oop_pc = hepc * (oop_share / 100.0)
                pharma_pc = oop_pc * pharma_oop_fraction
                # Affordability ratio: pharma spending as % of GDP per capita
                ratio = 100.0 * pharma_pc / gdppc
                affordability_ratios.append(ratio)

        if not affordability_ratios:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "Insufficient overlapping data for affordability calculation",
            }

        mean_ratio = float(np.mean(affordability_ratios))
        median_ratio = float(np.median(affordability_ratios))

        # Thresholds: >2% of GDP pc on medicines is unaffordable for low-income HH
        unaffordable = [r for r in affordability_ratios if r > 2.0]
        borderline = [r for r in affordability_ratios if 1.0 < r <= 2.0]

        n = len(affordability_ratios)
        stress = (len(unaffordable) * 1.0 + len(borderline) * 0.5) / n
        score = float(np.clip(stress * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "n_countries": n,
                "mean_pharma_oop_pct_gdppc": round(mean_ratio, 3),
                "median_pharma_oop_pct_gdppc": round(median_ratio, 3),
                "countries_unaffordable_gt2pct": len(unaffordable),
                "countries_borderline_1_2pct": len(borderline),
                "pct_unaffordable": round(100.0 * len(unaffordable) / n, 1),
                "pharma_share_of_oop_assumed": pharma_oop_fraction,
                "affordability_threshold_pct_gdppc": 2.0,
            },
        }
