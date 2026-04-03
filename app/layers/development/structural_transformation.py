"""Structural transformation: sectoral employment and productivity shifts.

Decomposes aggregate labor productivity growth into within-sector productivity
gains and between-sector reallocation effects (McMillan-Rodrik decomposition).
Tracks the shift from agriculture to manufacturing and services.

Key references:
    McMillan, M. & Rodrik, D. (2011). Globalization, structural change and
        productivity growth. NBER Working Paper 17143.
    Herrendorf, B., Rogerson, R. & Valentinyi, A. (2014). Growth and structural
        transformation. Handbook of Economic Growth, 2, 855-941.
    Diao, X., McMillan, M. & Rodrik, D. (2019). The recent growth boom in
        developing economies. Journal of Economic Perspectives, 33(2), 65-89.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class StructuralTransformation(LayerBase):
    layer_id = "l4"
    name = "Structural Transformation"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Decompose productivity growth into within and between components.

        Fetches sectoral value added shares and employment data. Performs
        the McMillan-Rodrik shift-share decomposition of aggregate
        productivity growth.

        Returns dict with score, within/between components, sectoral
        shares trajectory, and premature deindustrialization assessment.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Fetch sectoral value added (% of GDP)
        sector_series = {
            "NV.AGR.TOTL.ZS": "agriculture",
            "NV.IND.MANF.ZS": "manufacturing",
            "NV.IND.TOTL.ZS": "industry",
            "NV.SRV.TOTL.ZS": "services",
        }

        sector_data: dict[str, dict[str, dict[str, float]]] = {}  # iso -> year -> sector -> value
        for series_id, sector in sector_series.items():
            rows = await db.fetch_all(
                """
                SELECT ds.country_iso3, dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.series_id = ?
                  AND dp.value IS NOT NULL
                ORDER BY ds.country_iso3, dp.date
                """,
                (series_id,),
            )
            for r in rows:
                iso = r["country_iso3"]
                yr = r["date"][:4]
                sector_data.setdefault(iso, {}).setdefault(yr, {})[sector] = r["value"]

        # Fetch employment in agriculture (% of total)
        emp_agr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SL.AGR.EMPL.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )
        emp_agr: dict[str, dict[str, float]] = {}
        for r in emp_agr_rows:
            emp_agr.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Employment in industry
        emp_ind_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SL.IND.EMPL.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )
        emp_ind: dict[str, dict[str, float]] = {}
        for r in emp_ind_rows:
            emp_ind.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Employment in services
        emp_srv_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SL.SRV.EMPL.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )
        emp_srv: dict[str, dict[str, float]] = {}
        for r in emp_srv_rows:
            emp_srv.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]

        # Fetch GDP per capita for income context
        gdp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )
        gdp_dict = {r["country_iso3"]: r["value"] for r in gdp_rows} if gdp_rows else {}

        if not sector_data:
            return {"score": 50, "results": {"error": "no sectoral data"}}

        # McMillan-Rodrik decomposition for countries with employment + VA data
        country_results: dict[str, dict] = {}

        for iso in sector_data:
            years = sorted(sector_data[iso].keys())
            if len(years) < 5:
                continue

            # Check if we have employment data
            has_emp = iso in emp_agr and iso in emp_ind and iso in emp_srv

            if has_emp:
                emp_years = sorted(
                    set(emp_agr.get(iso, {}).keys())
                    & set(emp_ind.get(iso, {}).keys())
                    & set(emp_srv.get(iso, {}).keys())
                    & set(years)
                )

                if len(emp_years) >= 3:
                    # Compute shift-share decomposition
                    # within = sum_i (theta_i,0 * delta_pi_i)
                    # between = sum_i (pi_i,0 * delta_theta_i)
                    # where theta = employment share, pi = sector productivity (VA_share/emp_share)

                    y0 = emp_years[0]
                    yt = emp_years[-1]

                    sectors = ["agriculture", "industry", "services"]
                    emp_data = {
                        "agriculture": emp_agr[iso],
                        "industry": emp_ind[iso],
                        "services": emp_srv[iso],
                    }
                    va_map = {"agriculture": "agriculture", "industry": "industry", "services": "services"}

                    within = 0.0
                    between = 0.0
                    sector_details = {}

                    for s in sectors:
                        va_key = va_map[s]
                        if (y0 in emp_data[s] and yt in emp_data[s]
                                and y0 in sector_data[iso] and yt in sector_data[iso]
                                and va_key in sector_data[iso][y0]
                                and va_key in sector_data[iso][yt]):

                            theta_0 = emp_data[s][y0] / 100  # Employment share
                            theta_t = emp_data[s][yt] / 100
                            va_0 = sector_data[iso][y0][va_key]
                            va_t = sector_data[iso][yt][va_key]

                            # Relative productivity: VA share / employment share
                            pi_0 = va_0 / (theta_0 * 100) if theta_0 > 0 else 0
                            pi_t = va_t / (theta_t * 100) if theta_t > 0 else 0

                            within += theta_0 * (pi_t - pi_0)
                            between += pi_0 * (theta_t - theta_0)

                            sector_details[s] = {
                                "emp_share_initial": theta_0 * 100,
                                "emp_share_final": theta_t * 100,
                                "emp_share_change": (theta_t - theta_0) * 100,
                                "va_share_initial": va_0,
                                "va_share_final": va_t,
                                "rel_productivity_initial": pi_0,
                                "rel_productivity_final": pi_t,
                            }

                    total = within + between
                    country_results[iso] = {
                        "within_component": float(within),
                        "between_component": float(between),
                        "total_decomposition": float(total),
                        "between_share": float(between / total) if total != 0 else 0,
                        "growth_enhancing_reallocation": between > 0,
                        "sectors": sector_details,
                        "period": f"{y0}-{yt}",
                    }
                    continue

            # Fallback: just report sectoral shares over time
            first_yr = years[0]
            last_yr = years[-1]
            country_results[iso] = {
                "sectoral_shares": {
                    "initial": sector_data[iso].get(first_yr, {}),
                    "final": sector_data[iso].get(last_yr, {}),
                    "period": f"{first_yr}-{last_yr}",
                },
            }

        if not country_results:
            return {"score": 50, "results": {"error": "insufficient data for structural analysis"}}

        # Premature deindustrialization check
        premature_deind = None
        if country_iso3 and country_iso3 in sector_data and country_iso3 in gdp_dict:
            years = sorted(sector_data[country_iso3].keys())
            mfg_shares = [
                sector_data[country_iso3][y].get("manufacturing", 0)
                for y in years
                if "manufacturing" in sector_data[country_iso3].get(y, {})
            ]
            if len(mfg_shares) >= 3:
                peak_mfg = max(mfg_shares)
                latest_mfg = mfg_shares[-1]
                gdp_pc = gdp_dict[country_iso3]

                # Premature if manufacturing peaked below 18% or declining at low income
                premature_deind = {
                    "peak_manufacturing_share": peak_mfg,
                    "current_manufacturing_share": latest_mfg,
                    "gdp_per_capita": gdp_pc,
                    "premature": peak_mfg < 18 and gdp_pc < 10000,
                    "declining": latest_mfg < peak_mfg - 2,
                }

        # Target country
        target = country_results.get(country_iso3) if country_iso3 else None

        # Score
        if target and "between_component" in target:
            between = target["between_component"]
            if between > 0:
                score = max(15, 40 - between * 10)  # Growth-enhancing reallocation
            else:
                score = min(85, 60 + abs(between) * 10)  # Growth-reducing reallocation
        elif premature_deind and premature_deind["premature"]:
            score = 70
        else:
            score = 50

        score = float(np.clip(score, 0, 100))

        results = {
            "target": target,
            "premature_deindustrialization": premature_deind,
            "n_countries": len(country_results),
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
