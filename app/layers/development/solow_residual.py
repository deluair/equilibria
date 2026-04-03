"""Solow residual: TFP estimation via growth accounting.

Decomposes GDP growth into contributions from capital accumulation,
labor force growth, and total factor productivity (the Solow residual).
Uses the standard Cobb-Douglas production function Y = A * K^alpha * L^(1-alpha).

Key references:
    Solow, R. (1957). Technical change and the aggregate production function.
        Review of Economics and Statistics, 39(3), 312-320.
    Hall, R. & Jones, C. (1999). Why do some countries produce so much more
        output per worker than others? QJE, 114(1), 83-116.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SolowResidual(LayerBase):
    layer_id = "l4"
    name = "Solow Residual (TFP)"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate TFP via growth accounting decomposition.

        Fetches GDP, capital stock (or investment as proxy), and labor force
        data. Decomposes output growth into factor accumulation and the
        Solow residual (TFP growth).

        Returns dict with score, TFP growth rate, factor contributions,
        and capital share calibration.
        """
        country_iso3 = kwargs.get("country_iso3")
        alpha = kwargs.get("alpha", 0.33)  # Capital share

        # Fetch GDP, gross capital formation, and labor force
        series_map = {
            "NY.GDP.MKTP.KD": "gdp",
            "NE.GDI.TOTL.KD": "investment",
            "SL.TLF.TOTL.IN": "labor",
        }

        data: dict[str, dict[str, list[tuple[str, float]]]] = {}
        for series_id, label in series_map.items():
            where_clause = "AND ds.country_iso3 = ?" if country_iso3 else ""
            params = (series_id, country_iso3) if country_iso3 else (series_id,)
            rows = await db.fetch_all(
                f"""
                SELECT ds.country_iso3, dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.series_id = ?
                  {where_clause}
                  AND dp.value > 0
                ORDER BY ds.country_iso3, dp.date
                """,
                params,
            )
            for r in rows:
                iso = r["country_iso3"]
                data.setdefault(iso, {}).setdefault(label, []).append(
                    (r["date"], r["value"])
                )

        if not data:
            return {"score": 50, "results": {"error": "no growth accounting data"}}

        country_results = {}
        all_tfp_growth = []

        for iso, series in data.items():
            if "gdp" not in series or "labor" not in series:
                continue

            gdp_ts = sorted(series["gdp"], key=lambda x: x[0])
            labor_ts = sorted(series["labor"], key=lambda x: x[0])

            # Align by year
            gdp_by_year = {d[:4]: v for d, v in gdp_ts}
            labor_by_year = {d[:4]: v for d, v in labor_ts}

            common_years = sorted(set(gdp_by_year.keys()) & set(labor_by_year.keys()))
            if len(common_years) < 5:
                continue

            gdp_arr = np.array([gdp_by_year[y] for y in common_years])
            labor_arr = np.array([labor_by_year[y] for y in common_years])

            # Construct capital stock via perpetual inventory if investment available
            if "investment" in series:
                inv_ts = sorted(series["investment"], key=lambda x: x[0])
                inv_by_year = {d[:4]: v for d, v in inv_ts}
                inv_years = sorted(set(common_years) & set(inv_by_year.keys()))

                if len(inv_years) >= 5:
                    depreciation = 0.06
                    inv_arr = np.array([inv_by_year[y] for y in inv_years])
                    # Initial capital: K0 = I0 / (g + delta) where g = avg growth
                    avg_inv_growth = np.mean(np.diff(inv_arr) / inv_arr[:-1])
                    k0 = inv_arr[0] / (avg_inv_growth + depreciation) if (avg_inv_growth + depreciation) > 0 else inv_arr[0] * 10
                    capital = [k0]
                    for i in range(1, len(inv_arr)):
                        capital.append(capital[-1] * (1 - depreciation) + inv_arr[i])
                    capital_arr = np.array(capital)

                    # Recompute common years for capital
                    gdp_aligned = np.array([gdp_by_year[y] for y in inv_years])
                    labor_aligned = np.array([labor_by_year[y] for y in inv_years if y in labor_by_year])

                    min_len = min(len(gdp_aligned), len(labor_aligned), len(capital_arr))
                    gdp_aligned = gdp_aligned[:min_len]
                    labor_aligned = labor_aligned[:min_len]
                    capital_arr = capital_arr[:min_len]

                    if min_len >= 3:
                        # Growth rates
                        g_y = np.diff(np.log(gdp_aligned))
                        g_k = np.diff(np.log(capital_arr))
                        g_l = np.diff(np.log(labor_aligned))

                        # Solow residual: g_A = g_Y - alpha*g_K - (1-alpha)*g_L
                        g_a = g_y - alpha * g_k - (1 - alpha) * g_l

                        avg_tfp_growth = float(np.mean(g_a))
                        all_tfp_growth.append(avg_tfp_growth)

                        # Contributions to growth
                        avg_gdp_growth = float(np.mean(g_y))
                        capital_contrib = float(alpha * np.mean(g_k))
                        labor_contrib = float((1 - alpha) * np.mean(g_l))
                        tfp_contrib = avg_tfp_growth

                        # Share of growth from TFP
                        tfp_share = tfp_contrib / avg_gdp_growth if avg_gdp_growth != 0 else 0

                        country_results[iso] = {
                            "avg_gdp_growth": avg_gdp_growth,
                            "capital_contribution": capital_contrib,
                            "labor_contribution": labor_contrib,
                            "tfp_growth": avg_tfp_growth,
                            "tfp_share_of_growth": tfp_share,
                            "alpha": alpha,
                            "n_years": min_len,
                            "years": inv_years[:min_len],
                        }
                        continue

            # Fallback: simple output per worker growth
            gdp_per_worker = gdp_arr / labor_arr
            g_y_per_l = np.diff(np.log(gdp_per_worker))
            avg_growth = float(np.mean(g_y_per_l))
            all_tfp_growth.append(avg_growth)

            country_results[iso] = {
                "avg_output_per_worker_growth": avg_growth,
                "n_years": len(common_years),
                "note": "capital stock unavailable, reporting output per worker growth",
            }

        if not country_results:
            return {"score": 50, "results": {"error": "insufficient data for growth accounting"}}

        # Score based on TFP growth
        target = country_results.get(country_iso3, {}) if country_iso3 else {}
        tfp_rate = target.get("tfp_growth", target.get("avg_output_per_worker_growth"))

        if tfp_rate is not None:
            # High TFP growth = low score (good), negative = high score (bad)
            if tfp_rate > 0.02:
                score = 20
            elif tfp_rate > 0.01:
                score = 35
            elif tfp_rate > 0:
                score = 50
            elif tfp_rate > -0.01:
                score = 65
            else:
                score = 80
        else:
            # Use cross-country average
            avg = np.mean(all_tfp_growth) if all_tfp_growth else 0
            score = 50 - avg * 1000
            score = float(np.clip(score, 10, 90))

        results = {
            "countries": country_results,
            "cross_country_avg_tfp": float(np.mean(all_tfp_growth)) if all_tfp_growth else None,
            "cross_country_std_tfp": float(np.std(all_tfp_growth)) if len(all_tfp_growth) > 1 else None,
            "alpha": alpha,
            "n_countries": len(country_results),
            "country_iso3": country_iso3,
        }

        return {"score": float(np.clip(score, 0, 100)), "results": results}
