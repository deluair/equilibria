"""Urbanization development gap: urban-rural income gap proxy.

Examines whether a country's urbanization rate is low relative to its income
level. Low urbanization for a given GDP per capita signals urban-rural gaps
and uneven spatial development.

Key references:
    Henderson, J.V. (2005). Urbanization and growth. Handbook of Economic
        Growth, Vol. 1B, 1543-1591.
    World Bank (2009). World Development Report: Reshaping Economic Geography.
"""

from __future__ import annotations

import numpy as np
import statsmodels.api as sm

from app.layers.base import LayerBase


class UrbanizationDevelopment(LayerBase):
    layer_id = "l4"
    name = "Urbanization Development Gap"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Urban-rural gap: urbanization rate relative to income level.

        Queries SP.URB.TOTL.IN.ZS (urban population % of total) and
        NY.GDP.PCAP.KD (GDP per capita). Fits a cross-country regression
        of urbanization on log income; residual < 0 means under-urbanized
        for income level (stress).

        Returns dict with score, urbanization rate, predicted rate, residual,
        and interpretation.
        """
        country_iso3 = kwargs.get("country_iso3")

        urban_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SP.URB.TOTL.IN.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        income_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not urban_rows or not income_rows:
            return {"score": 50, "results": {"error": "insufficient urbanization or income data"}}

        urban_data: dict[str, dict[str, float]] = {}
        for r in urban_rows:
            urban_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        income_data: dict[str, dict[str, float]] = {}
        for r in income_rows:
            income_data.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Build cross-section: latest available values
        urban_vals, log_income_vals, isos = [], [], []
        for iso in set(urban_data.keys()) & set(income_data.keys()):
            u_years = sorted(urban_data[iso].keys())
            i_years = sorted(income_data[iso].keys())
            if not u_years or not i_years:
                continue
            u_val = urban_data[iso][u_years[-1]]
            i_val = income_data[iso][i_years[-1]]
            if u_val is not None and i_val is not None and i_val > 0:
                urban_vals.append(u_val)
                log_income_vals.append(np.log(i_val))
                isos.append(iso)

        if len(urban_vals) < 20:
            return {"score": 50, "results": {"error": "insufficient countries for urbanization regression"}}

        y = np.array(urban_vals)
        X = sm.add_constant(np.array(log_income_vals))
        model = sm.OLS(y, X).fit(cov_type="HC1")
        predicted = model.predict(X)
        residuals = y - predicted

        iso_to_residual = dict(zip(isos, residuals.tolist()))

        # Global stats
        under_urbanized = [iso for iso, res in iso_to_residual.items() if res < -10]

        # Target country
        target_analysis = None
        score = 35.0

        if country_iso3 and country_iso3 in urban_data and country_iso3 in income_data:
            u_years = sorted(urban_data[country_iso3].keys())
            i_years = sorted(income_data[country_iso3].keys())
            if u_years and i_years:
                actual_urban = urban_data[country_iso3][u_years[-1]]
                actual_income = income_data[country_iso3][i_years[-1]]
                if actual_income and actual_income > 0:
                    pred_urban = float(model.params[0] + model.params[1] * np.log(actual_income))
                    residual = actual_urban - pred_urban
                    # Negative residual = under-urbanized for income level
                    raw_score = max(0.0, -residual) * 1.5
                    score = float(np.clip(raw_score, 0, 100))
                    target_analysis = {
                        "actual_urban_pct": actual_urban,
                        "predicted_urban_pct": pred_urban,
                        "residual": residual,
                        "under_urbanized": residual < -5,
                        "gdp_pc_kd": actual_income,
                    }
        elif iso_to_residual:
            # Global context score: fraction of countries under-urbanized
            frac = len(under_urbanized) / len(isos)
            score = float(np.clip(frac * 70, 0, 100))

        return {
            "score": score,
            "results": {
                "regression_r_sq": float(model.rsquared),
                "regression_n_obs": int(model.nobs),
                "under_urbanized_countries": under_urbanized[:10],
                "target": target_analysis,
                "country_iso3": country_iso3,
            },
        }
