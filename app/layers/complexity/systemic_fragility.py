"""Systemic Fragility module.

Triple-stress composite measuring cross-sector volatility, external debt burden,
and political instability. Each component contributes to overall fragility.

Composite = (1/3) * volatility_stress + (1/3) * debt_stress + (1/3) * instability_stress

Sources:
  WDI NY.GDP.MKTP.KD.ZG (GDP growth std dev for volatility)
  WDI DT.DOD.DECT.GD.ZS (external debt stocks % GNI)
  WDI PV.EST (political stability / no violence, -2.5 to +2.5)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SystemicFragility(LayerBase):
    layer_id = "lCP"
    name = "Systemic Fragility"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        # --- GDP growth volatility ---
        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        # --- External debt ---
        debt_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'DT.DOD.DECT.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        # --- Political stability ---
        pv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'PV.EST'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        if not gdp_rows and not debt_rows and not pv_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        components: list[float] = []
        details: dict = {}

        # Volatility stress: std dev of GDP growth. >5% std = high stress.
        if gdp_rows and len(gdp_rows) >= 5:
            gdp_vals = np.array([float(r["value"]) for r in gdp_rows])
            gdp_std = float(np.std(gdp_vals))
            volatility_stress = min(1.0, gdp_std / 5.0)
            components.append(volatility_stress)
            details["gdp_growth_std_pct"] = round(gdp_std, 3)
            details["gdp_growth_mean_pct"] = round(float(np.mean(gdp_vals)), 3)
            details["gdp_period"] = f"{gdp_rows[0]['date']} to {gdp_rows[-1]['date']}"
        else:
            volatility_stress = None

        # Debt stress: external debt % GNI. >100% = severe. Normalize 0-150%.
        if debt_rows:
            debt_val = float(debt_rows[0]["value"])
            debt_stress = min(1.0, debt_val / 150.0)
            components.append(debt_stress)
            details["external_debt_pct_gni"] = round(debt_val, 2)
            details["debt_date"] = debt_rows[0]["date"]
        else:
            debt_stress = None

        # Instability stress: PV.EST in [-2.5, +2.5]. Normalize to [0, 1] stress.
        if pv_rows:
            pv_val = float(pv_rows[0]["value"])
            instability_stress = ((-pv_val) + 2.5) / 5.0
            instability_stress = max(0.0, min(1.0, instability_stress))
            components.append(instability_stress)
            details["political_stability_pv_est"] = round(pv_val, 4)
            details["pv_date"] = pv_rows[0]["date"]
        else:
            instability_stress = None

        if not components:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no components available"}

        composite_stress = float(np.mean(components))
        score = float(min(100.0, max(0.0, composite_stress * 100.0)))

        return {
            "score": round(score, 1),
            "country": country,
            "composite_stress": round(composite_stress, 4),
            "n_components": len(components),
            "volatility_stress_0_1": round(volatility_stress, 4) if volatility_stress is not None else None,
            "debt_stress_0_1": round(debt_stress, 4) if debt_stress is not None else None,
            "instability_stress_0_1": round(instability_stress, 4) if instability_stress is not None else None,
            "details": details,
            "interpretation": (
                "High score = systemic fragility (volatile growth + high debt + political instability). "
                "Low score = stable, low-debt, politically stable system."
            ),
            "_citation": "World Bank WDI: NY.GDP.MKTP.KD.ZG, DT.DOD.DECT.GD.ZS, PV.EST",
        }
