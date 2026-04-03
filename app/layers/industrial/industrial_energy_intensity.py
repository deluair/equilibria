"""Industrial energy intensity: energy use relative to industrial output.

Energy intensity measures the energy required to generate one unit of economic
output. High industrial energy intensity indicates technological inefficiency,
reliance on energy-intensive processes, or subsidized fossil fuels that
suppress conservation incentives. It is a key determinant of industrial
competitiveness in a carbon-constrained global economy.

As carbon pricing and border adjustments (CBAM) expand, energy-intensive
industries face growing cost and trade headwinds. High intensity also signals
vulnerability to energy price shocks: a country that uses more energy per unit
of industrial output suffers more when oil or gas prices spike.

Indicator construction:
    energy_per_capita: EG.USE.PCAP.KG.OE (kg of oil equivalent per capita)
    industry_share:    NV.IND.TOTL.ZS (industry, % of GDP)
    intensity_proxy = energy_per_capita / max(industry_share, 1)
    (higher = more energy per unit of industrial output)

Empirical benchmarks for intensity_proxy (kg OE per % industrial GDP):
    < 30:   efficient (Scandinavian norm)
    30-60:  moderate
    60-100: high intensity
    > 100:  very high (energy-subsidizing or post-Soviet norm)

Score formula:
    score = clip((intensity_proxy - 20) * 0.8, 0, 100)
    Intensity of 20 -> score ~0 (STABLE); intensity 145 -> score 100 (CRISIS).

References:
    IEA (2023). Energy Efficiency 2023. Paris: International Energy Agency.
    World Bank (2022). Green Competitiveness Index.
    World Bank WDI: EG.USE.PCAP.KG.OE, NV.IND.TOTL.ZS.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class IndustrialEnergyIntensity(LayerBase):
    layer_id = "l14"
    name = "Industrial Energy Intensity"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        energy_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'EG.USE.PCAP.KG.OE'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        industry_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.series_id = 'NV.IND.TOTL.ZS'
              AND ds.country_iso3 = ?
              AND dp.value IS NOT NULL
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not energy_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no energy use per capita data",
            }

        energy_dates = [r["date"] for r in energy_rows]
        energy_vals = np.array([float(r["value"]) for r in energy_rows], dtype=float)

        latest_energy = float(energy_vals[-1])
        industry_pct = float(industry_rows[-1]["value"]) if industry_rows else None
        industry_year = industry_rows[-1]["date"] if industry_rows else None

        # Intensity proxy
        denom = max(float(industry_pct), 1.0) if industry_pct is not None else 30.0
        intensity = latest_energy / denom

        score = float(np.clip((intensity - 20.0) * 0.8, 0.0, 100.0))

        trend = None
        if len(energy_vals) >= 3:
            t = np.arange(len(energy_vals), dtype=float)
            slope, _, r_value, p_value, _ = linregress(t, energy_vals)
            trend = {
                "slope_kg_oe_per_year": round(float(slope), 2),
                "r_squared": round(float(r_value ** 2), 4),
                "p_value": round(float(p_value), 4),
                "direction": "improving" if float(slope) < 0 else "deteriorating",
            }

        return {
            "score": round(score, 2),
            "country": country,
            "energy_per_capita_kg_oe": round(latest_energy, 1),
            "energy_year": energy_dates[-1],
            "industry_pct_gdp": round(industry_pct, 2) if industry_pct is not None else None,
            "industry_year": industry_year,
            "intensity_proxy": round(intensity, 2),
            "n_obs_energy": len(energy_vals),
            "intensity_tier": (
                "efficient" if intensity < 30
                else "moderate" if intensity < 60
                else "high" if intensity < 100
                else "very high"
            ),
            "trend": trend,
        }
