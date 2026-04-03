"""Health fiscal space: government capacity to expand health spending.

Estimates fiscal space for health -- the room within a country's budget to
expand public health expenditure without jeopardizing fiscal sustainability.
Key dimensions: government revenue as % of GDP, public health share of
government budget, and revenue trend.

Fiscal space for health is constrained when: (1) government revenue is low
relative to GDP, (2) the government health expenditure share of total
government spending is already high, or (3) fiscal deficits are large.

Key references:
    Heller, P.S. (2006). The concept of fiscal space. IMF Policy Discussion
        Paper 06/4.
    Tandon, A. & Cashin, C. (2010). Assessing public expenditure on health
        from a fiscal space perspective. World Bank Health Nutrition and
        Population Discussion Paper.
    Moreno-Serra, R. & Wagstaff, A. (2010). System-wide impacts of hospital
        payment reforms: evidence from central and eastern Europe and central
        Asia. Journal of Health Economics, 29(4), 585-602.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthFiscalSpace(LayerBase):
    layer_id = "lHF"
    name = "Health Fiscal Space"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate fiscal space for health spending expansion.

        Fetches government revenue as % of GDP (GC.REV.XGRT.GD.ZS),
        government health expenditure as % of government expenditure
        (SH.XPD.GHED.GE.ZS), and total government expenditure as % of GDP
        (GC.XPN.TOTL.GD.ZS) to assess fiscal capacity for health investment.

        Returns dict with score, signal, and fiscal space indicators.
        """
        rev_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'GC.REV.XGRT.GD.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        ghed_ge_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.GHED.GE.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        govexp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'GC.XPN.TOTL.GD.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        if not rev_rows and not ghed_ge_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No government revenue or health budget share data in DB",
            }

        def _latest(rows) -> dict[str, float]:
            out: dict[str, float] = {}
            for row in rows:
                iso = row["country_iso3"]
                if iso not in out and row["value"] is not None:
                    out[iso] = float(row["value"])
            return out

        rev_data = _latest(rev_rows)
        ghed_ge_data = _latest(ghed_ge_rows)
        govexp_data = _latest(govexp_rows)

        # Fiscal space score components
        # 1. Low government revenue = constrained fiscal space
        # 2. High GHED/GE = little room to expand health share
        # 3. High government expenditure/GDP with low revenue = deficit pressure

        scores_per_country: list[float] = []
        all_isos = set(rev_data.keys()) | set(ghed_ge_data.keys())

        for iso in all_isos:
            rev = rev_data.get(iso)
            ghed_ge = ghed_ge_data.get(iso)
            govexp = govexp_data.get(iso)

            component_score = 0.0
            n_components = 0

            # Low revenue = constrained space (< 15% GDP is very low)
            if rev is not None:
                if rev < 10:
                    component_score += 80
                elif rev < 15:
                    component_score += 60
                elif rev < 20:
                    component_score += 40
                elif rev < 25:
                    component_score += 20
                n_components += 1

            # High health budget share already = less room (> 15% of govt budget)
            if ghed_ge is not None:
                if ghed_ge > 20:
                    component_score += 20  # high but shows commitment
                elif ghed_ge < 5:
                    component_score += 60  # very low priority = fiscal space risk
                elif ghed_ge < 10:
                    component_score += 40
                n_components += 1

            if n_components > 0:
                scores_per_country.append(component_score / n_components)

        if not scores_per_country:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid fiscal indicators after combining sources",
            }

        score = float(np.clip(np.mean(scores_per_country), 0, 100))

        mean_rev = float(np.mean(list(rev_data.values()))) if rev_data else None
        mean_ghed_ge = float(np.mean(list(ghed_ge_data.values()))) if ghed_ge_data else None

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "n_countries_assessed": len(scores_per_country),
                "mean_govt_revenue_pct_gdp": round(mean_rev, 2) if mean_rev is not None else None,
                "mean_health_share_govt_budget_pct": round(mean_ghed_ge, 2) if mean_ghed_ge is not None else None,
                "revenue_data_countries": len(rev_data),
                "health_budget_share_countries": len(ghed_ge_data),
                "govt_expenditure_data_countries": len(govexp_data),
            },
        }
