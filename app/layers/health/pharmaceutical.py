"""Pharmaceutical economics: pricing, generics, patents, and TRIPS.

Constructs drug price indices across countries, estimates generic entry
effects on branded drug prices, analyzes patent cliff impacts on
pharmaceutical markets, and assesses TRIPS flexibility utilization for
developing countries.

Key references:
    Grabowski, H. & Vernon, J. (1992). Brand loyalty, entry, and price
        competition in pharmaceuticals after the 1984 Drug Price Competition
        and Patent Term Restoration Act. JLE, 35(2), 331-350.
    Reiffen, D. & Ward, M. (2005). Generic drug industry dynamics. REStat,
        87(1), 37-49.
    Scherer, F.M. (2004). The pharmaceutical industry: prices and progress.
        NEJM, 351(9), 927-932.
    Chaudhuri, S., Goldberg, P.K. & Jia, P. (2006). Estimating the effects
        of global patent protection in pharmaceuticals: a case study of
        quinolones in India. AER, 96(5), 1477-1514.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class PharmaceuticalEconomics(LayerBase):
    layer_id = "l8"
    name = "Pharmaceutical Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Analyze pharmaceutical market dynamics.

        Fetches pharmaceutical expenditure, health spending composition,
        GDP per capita, and patent-related indicators. Estimates price
        indices, generic entry effects, patent cliff exposure, and TRIPS
        flexibility usage.

        Returns dict with score, drug price index, generic entry model,
        patent cliff analysis, and TRIPS flexibility assessment.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Health expenditure per capita
        hepc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.CHEX.PC.CD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # GDP per capita
        gdppc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # OOP spending as % of CHE (proxy for pharmaceutical burden)
        oop_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.OOPC.CH.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # Patent applications (residents) -- proxy for pharma innovation capacity
        patent_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'IP.PAT.RESD'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        # R&D expenditure as % of GDP
        rd_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'GB.XPD.RSDV.GD.ZS'
            ORDER BY ds.country_iso3, dp.date
            """
        )

        if not hepc_rows or not gdppc_rows:
            return {"score": 50, "results": {"error": "no health expenditure or GDP data"}}

        def _index(rows):
            out: dict[str, dict[str, float]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], {})[r["date"][:4]] = r["value"]
            return out

        hepc_data = _index(hepc_rows)
        gdppc_data = _index(gdppc_rows)
        oop_data = _index(oop_rows) if oop_rows else {}
        patent_data = _index(patent_rows) if patent_rows else {}
        rd_data = _index(rd_rows) if rd_rows else {}

        # --- Drug price index (cross-country) ---
        # Use health spending per capita relative to income as price proxy.
        # Richer countries spend more, but price index = residual after income.
        price_index = None
        hepc_list, gdp_list, iso_list = [], [], []
        for iso in set(hepc_data.keys()) & set(gdppc_data.keys()):
            h_years = hepc_data[iso]
            g_years = gdppc_data[iso]
            common = sorted(set(h_years.keys()) & set(g_years.keys()))
            if common:
                yr = common[-1]
                h_val = h_years[yr]
                g_val = g_years[yr]
                if h_val and h_val > 0 and g_val and g_val > 0:
                    hepc_list.append(np.log(h_val))
                    gdp_list.append(np.log(g_val))
                    iso_list.append(iso)

        if len(hepc_list) >= 20:
            y = np.array(hepc_list)
            x = np.array(gdp_list)
            X = np.column_stack([np.ones(len(x)), x])
            beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
            residuals = y - X @ beta

            # Residual > 0 means spending more than income predicts (high prices/usage)
            target_residual = None
            if country_iso3 and country_iso3 in iso_list:
                idx = iso_list.index(country_iso3)
                target_residual = float(residuals[idx])

            # Rank countries by price index residual
            ranked = sorted(zip(iso_list, residuals.tolist()), key=lambda x: -x[1])

            price_index = {
                "income_elasticity": float(beta[1]),
                "n_countries": len(iso_list),
                "target_residual": target_residual,
                "target_interpretation": (
                    "above_predicted" if target_residual and target_residual > 0
                    else "below_predicted" if target_residual and target_residual < 0
                    else None
                ),
                "highest_price": [{"iso3": c, "residual": round(r, 3)} for c, r in ranked[:5]],
                "lowest_price": [{"iso3": c, "residual": round(r, 3)} for c, r in ranked[-5:]],
            }

        # --- Generic entry effects ---
        # Cross-country model: countries with lower patent protection (lower R&D, fewer
        # patents) should have lower health spending, ceteris paribus -- generic availability.
        # Grabowski-Vernon: each generic entrant reduces price by ~2-5%.
        generic_effects = None
        patent_list, spend_list_g, gdp_list_g = [], [], []

        for iso in set(patent_data.keys()) & set(hepc_data.keys()) & set(gdppc_data.keys()):
            p_years = patent_data[iso]
            h_years = hepc_data[iso]
            g_years = gdppc_data[iso]
            common = sorted(set(p_years.keys()) & set(h_years.keys()) & set(g_years.keys()))
            if common:
                yr = common[-1]
                p_val = p_years[yr]
                h_val = h_years[yr]
                g_val = g_years[yr]
                if p_val is not None and h_val and h_val > 0 and g_val and g_val > 0:
                    patent_list.append(np.log(max(p_val, 1)))
                    spend_list_g.append(np.log(h_val))
                    gdp_list_g.append(np.log(g_val))

        if len(patent_list) >= 15:
            y_g = np.array(spend_list_g)
            pat_arr = np.array(patent_list)
            gdp_arr_g = np.array(gdp_list_g)
            X_g = np.column_stack([np.ones(len(pat_arr)), pat_arr, gdp_arr_g])
            beta_g, _, _, _ = np.linalg.lstsq(X_g, y_g, rcond=None)

            y_hat = X_g @ beta_g
            ss_res = np.sum((y_g - y_hat) ** 2)
            ss_tot = np.sum((y_g - np.mean(y_g)) ** 2)
            r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0

            generic_effects = {
                "patent_spending_elasticity": float(beta_g[1]),
                "gdppc_coef": float(beta_g[2]),
                "r_squared": float(r_sq),
                "n_countries": len(patent_list),
                "interpretation": (
                    "More patents associated with higher spending"
                    if beta_g[1] > 0
                    else "Weak or negative patent-spending link"
                ),
            }

        # --- Patent cliff analysis ---
        # Track R&D spending trajectory. Declining R&D = potential patent cliff
        # (pipeline drying up, future generic competition).
        patent_cliff = None
        if country_iso3 and country_iso3 in rd_data:
            rd_years = rd_data[country_iso3]
            yrs = sorted(rd_years.keys())
            if len(yrs) >= 3:
                vals = [rd_years[y] for y in yrs if rd_years[y] is not None]
                if len(vals) >= 3:
                    t = np.arange(len(vals), dtype=float)
                    slope, intercept, r_val, p_val, se = stats.linregress(t, vals)

                    patent_cliff = {
                        "rd_pct_gdp_latest": float(vals[-1]),
                        "rd_trend_slope": float(slope),
                        "rd_trend_pval": float(p_val),
                        "rd_declining": bool(slope < 0 and p_val < 0.1),
                        "years_covered": len(vals),
                    }

        # --- TRIPS flexibility utilization ---
        # For developing countries: assess whether the country uses TRIPS
        # flexibilities (compulsory licensing, parallel imports).
        # Proxy: low-patent countries with lower OOP and health spending ratios
        # suggest generic-friendly policies.
        trips = None
        if country_iso3:
            gdppc_c = gdppc_data.get(country_iso3, {})
            oop_c = oop_data.get(country_iso3, {})
            patent_c = patent_data.get(country_iso3, {})

            if gdppc_c:
                latest_yr = sorted(gdppc_c.keys())[-1]
                gdp_val = gdppc_c[latest_yr]
                is_developing = gdp_val < 12000  # rough threshold

                oop_val = None
                if oop_c and latest_yr in oop_c:
                    oop_val = oop_c[latest_yr]

                patent_val = None
                if patent_c and latest_yr in patent_c:
                    patent_val = patent_c[latest_yr]

                # TRIPS flexibility score: developing + low OOP = likely using flexibilities
                flexibility_score = 0
                if is_developing:
                    flexibility_score += 30
                    if oop_val and oop_val < 30:
                        flexibility_score += 20  # financial protection suggests policy
                    if patent_val is not None and patent_val < 1000:
                        flexibility_score += 20  # few patents = generic-friendly
                    # Additional: low health spending per capita
                    hepc_c = hepc_data.get(country_iso3, {})
                    if hepc_c and latest_yr in hepc_c:
                        if hepc_c[latest_yr] < 200:
                            flexibility_score += 15

                trips = {
                    "gdp_per_capita": float(gdp_val),
                    "is_developing": is_developing,
                    "oop_share": float(oop_val) if oop_val is not None else None,
                    "patent_applications": float(patent_val) if patent_val is not None else None,
                    "flexibility_utilization_score": flexibility_score,
                    "flexibility_tier": (
                        "high" if flexibility_score >= 60
                        else "moderate" if flexibility_score >= 30
                        else "low"
                    ),
                }

        # --- Score ---
        score = 35
        if price_index and price_index["target_residual"] is not None:
            if price_index["target_residual"] > 0.5:
                score += 15  # overspending relative to income
            elif price_index["target_residual"] > 0.2:
                score += 8

        if trips:
            if trips["is_developing"] and trips["oop_share"] and trips["oop_share"] > 40:
                score += 20  # developing country with high OOP = pharma access problem
            elif trips["is_developing"]:
                score += 10

        if patent_cliff and patent_cliff["rd_declining"]:
            score += 10

        score = float(np.clip(score, 0, 100))

        results = {
            "drug_price_index": price_index,
            "generic_entry_effects": generic_effects,
            "patent_cliff": patent_cliff,
            "trips_flexibility": trips,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
