"""Health workforce economics: density and wage adequacy.

Analyzes the economics of the health workforce including worker density
(physicians, nurses, midwives per 1,000 population) and wage adequacy
relative to GDP per capita. Health worker shortages and wage inadequacy
are primary drivers of health system underperformance.

WHO minimum threshold: 4.45 health workers per 1,000 population (physicians +
nurses/midwives) to achieve 80% coverage of essential health services.

Key references:
    WHO (2016). Global strategy on human resources for health: workforce 2030.
    Scheil-Adlung, X. (2013). Health workforce: a global supply and demand
        analysis. ILO ESS Working Paper No. 40.
    Dal Poz, M.R. et al. (2009). Handbook on monitoring and evaluation of
        human resources for health. WHO.
    Campbell, J. et al. (2013). A universal truth: no health without a
        workforce. Third Global Forum on Human Resources for Health.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthWorkforceEconomics(LayerBase):
    layer_id = "lHF"
    name = "Health Workforce Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Compute health workforce density and wage adequacy.

        Fetches physician density (SH.MED.PHYS.ZS), nursing and midwifery
        density (SH.MED.NUMW.P3), and GDP per capita (NY.GDP.PCAP.KD) to
        assess workforce economics. Higher density relative to WHO threshold
        and adequate GDP-relative wages signal better workforce sustainability.

        Returns dict with score, signal, and workforce economics metrics.
        """
        phys_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.MED.PHYS.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        nurse_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.MED.NUMW.P3'
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

        if not phys_rows and not nurse_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No health workforce density data in DB",
            }

        def _latest(rows) -> dict[str, float]:
            out: dict[str, float] = {}
            for row in rows:
                iso = row["country_iso3"]
                if iso not in out and row["value"] is not None:
                    out[iso] = float(row["value"])
            return out

        phys_data = _latest(phys_rows)
        nurse_data = _latest(nurse_rows)
        gdppc_data = _latest(gdppc_rows)

        # WHO threshold: 4.45 health workers per 1,000 population
        who_threshold = 4.45

        # Combine physician + nurse density
        density: dict[str, float] = {}
        for iso in set(phys_data.keys()) | set(nurse_data.keys()):
            p = phys_data.get(iso, 0.0) or 0.0
            n = nurse_data.get(iso, 0.0) or 0.0
            # Physician density is per 1,000; nurse per 1,000 (SH.MED.NUMW.P3)
            density[iso] = p + n

        if not density:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid workforce density after combining physician and nurse data",
            }

        densities = list(density.values())
        below_who = [v for v in densities if v < who_threshold]
        n = len(densities)

        mean_density = float(np.mean(densities))
        median_density = float(np.median(densities))

        # Wage adequacy proxy: health expenditure per capita relative to GDP per capita
        # Higher ratio with low density = wage pressure; lower ratio = wage inadequacy risk
        hepc_gdppc_ratios: list[float] = []
        for iso in set(density.keys()) & set(gdppc_data.keys()):
            gdppc = gdppc_data[iso]
            d = density[iso]
            if gdppc > 0 and d > 0:
                # Proxy: lower density relative to income suggests wage inadequacy pull
                # (workers leave for better-paying sectors or countries)
                ratio = d / (gdppc / 10000.0)  # density per 10k USD GDP pc
                hepc_gdppc_ratios.append(ratio)

        # Score: higher % below WHO threshold = worse workforce economics
        shortage_score = 100.0 * len(below_who) / n if n > 0 else 50.0
        score = float(np.clip(shortage_score, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "n_countries": n,
                "who_threshold_per_1000": who_threshold,
                "mean_hw_density_per_1000": round(mean_density, 3),
                "median_hw_density_per_1000": round(median_density, 3),
                "countries_below_who_threshold": len(below_who),
                "pct_below_who_threshold": round(100.0 * len(below_who) / n, 1),
                "physician_countries": len(phys_data),
                "nurse_countries": len(nurse_data),
                "wage_proxy_countries": len(hepc_gdppc_ratios),
            },
        }
