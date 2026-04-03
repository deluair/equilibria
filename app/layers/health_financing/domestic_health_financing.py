"""Domestic vs external health financing ratio.

Measures the degree to which health systems are funded domestically versus
relying on external (donor/aid) financing. High external dependence signals
fiscal fragility and sustainability risk for health programs.

The domestic financing ratio is: GGHE / CHE. External financing dependency
is proxied by (CHE - GGHE - private_domestic) / CHE.

Key references:
    Dieleman, J.L. et al. (2016). Financing global health 2015: development
        assistance steady on the path to new global goals. IHME.
    WHO (2014). Making fair choices on the path to universal health coverage.
        Report of the WHO Consultative Group on Equity and UHC.
    Spicer, N. et al. (2010). National and subnational HIV/AIDS coordination:
        are global health initiatives closing the gap? Globalization and Health.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DomesticHealthFinancing(LayerBase):
    layer_id = "lHF"
    name = "Domestic Health Financing"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Compute domestic vs external health financing ratio.

        Fetches domestic general government health expenditure as % of CHE
        (SH.XPD.GHED.CH.ZS) and domestic private expenditure as % of CHE
        (SH.XPD.PVTD.CH.ZS). The sum of domestic public and private shares
        proxies total domestic financing; the remainder is external.

        Returns dict with score, signal, and domestic/external financing metrics.
        """
        gghe_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.GHED.CH.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        pvtd_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.PVTD.CH.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        if not gghe_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No government health expenditure data in DB",
            }

        def _latest(rows) -> dict[str, float]:
            out: dict[str, float] = {}
            for row in rows:
                iso = row["country_iso3"]
                if iso not in out and row["value"] is not None:
                    out[iso] = float(row["value"])
            return out

        gghe_data = _latest(gghe_rows)
        pvtd_data = _latest(pvtd_rows)

        # Compute domestic financing rate per country
        domestic_rates: dict[str, float] = {}
        external_rates: dict[str, float] = {}

        for iso, gghe_val in gghe_data.items():
            pvtd_val = pvtd_data.get(iso, 0.0) or 0.0
            domestic = min(100.0, gghe_val + pvtd_val)
            external = max(0.0, 100.0 - domestic)
            domestic_rates[iso] = domestic
            external_rates[iso] = external

        if not domestic_rates:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid domestic financing values",
            }

        dom_vals = list(domestic_rates.values())
        ext_vals = list(external_rates.values())
        n = len(dom_vals)

        mean_domestic = float(np.mean(dom_vals))
        mean_external = float(np.mean(ext_vals))

        # High external dependence thresholds
        high_dependence = [iso for iso, v in external_rates.items() if v > 40]
        moderate_dependence = [iso for iso, v in external_rates.items() if 15 < v <= 40]

        # Score: higher external dependence = higher financing fragility risk
        # Weight high dependence countries more heavily
        fragility = (len(high_dependence) * 1.0 + len(moderate_dependence) * 0.4) / n
        score = float(np.clip(fragility * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "n_countries": n,
                "mean_domestic_financing_pct": round(mean_domestic, 2),
                "mean_external_financing_pct": round(mean_external, 2),
                "countries_high_external_dependence_gt40pct": len(high_dependence),
                "countries_moderate_external_dependence_15_40pct": len(moderate_dependence),
                "pct_high_external_dependence": round(100.0 * len(high_dependence) / n, 1),
                "sources_combined": {
                    "gghe_countries": len(gghe_data),
                    "private_domestic_countries": len(pvtd_data),
                },
            },
        }
