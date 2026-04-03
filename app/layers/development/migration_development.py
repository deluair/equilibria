"""Migration and development nexus: brain drain/gain, remittances, diaspora FDI.

Four analytical dimensions:

1. Brain drain vs. brain gain: emigration of high-skilled workers reduces
   human capital stock (Bhagwati & Hamada 1974) but can trigger increased
   domestic education investment if emigration probability is nonzero
   (Mountford 1997; Beine-Docquier-Rapoport 2008 brain gain channel).
   Net effect estimated from skilled emigration rates and domestic tertiary
   enrollment response.

2. Diaspora effects on FDI: migrants serve as bridges reducing information
   asymmetries for foreign investors in home countries. Javorcik et al. (2011)
   estimate ~20% of FDI from US to migrant-origin countries is attributable
   to diaspora networks. Measured via correlation between migrant stock in
   OECD and inward FDI stock.

3. Remittance-growth nexus: remittances average 6.5% of GDP in developing
   countries (2023). Aggregate macroeconomic growth effect is disputed
   (Chami et al. 2003 find negative/zero; Catrinescu et al. 2009 find positive
   conditional on institutions). Current-account stabilization role is
   well-documented (counter-cyclical behavior confirmed in multiple panels).

4. Return migration human capital: return migrants bring back foreign-acquired
   skills, networks, and business practices. Dustmann-Weiss (2007) document
   wage premium for returnees in source countries. Measured via return
   migration rates and wage differentials for returnees vs. non-migrants.

References:
    Beine, M., Docquier, F. & Rapoport, H. (2008). Brain drain and human
        capital formation in developing countries. Economic Journal 118(528).
    Javorcik, B. et al. (2011). Migrant networks and foreign direct investment.
        Journal of Development Economics 94(2): 231-241.
    Chami, R., Fullenkamp, C. & Jahjah, S. (2003). Are immigrant remittance
        flows a source of capital for development? IMF Staff Papers 52(1).
    Catrinescu, N. et al. (2009). Remittances, institutions, and economic
        growth. World Development 37(1): 81-92.
    Dustmann, C. & Weiss, Y. (2007). Return migration: Theory and empirical
        evidence from the UK. British Journal of Industrial Relations 45(2).

Score: high skilled emigration + low return + low remittance utilization
-> STRESS. Brain gain channel active + high remittances + diaspora FDI -> STABLE.
"""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class MigrationDevelopment(LayerBase):
    layer_id = "l4"
    name = "Migration & Development"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        # Remittances (% GDP)
        remit_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'BX.TRF.PWKR.DT.GD.ZS'
              AND dp.value IS NOT NULL
              AND (:country IS NULL OR ds.country_iso3 = :country)
            ORDER BY ds.country_iso3, dp.date ASC
            """,
            {"country": country_iso3},
        )

        # Net migration rate
        net_migr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SM.POP.NETM'
              AND dp.value IS NOT NULL
              AND (:country IS NULL OR ds.country_iso3 = :country)
            ORDER BY ds.country_iso3, dp.date ASC
            """,
            {"country": country_iso3},
        )

        # FDI inflows (% GDP)
        fdi_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'BX.KLT.DINV.WD.GD.ZS'
              AND dp.value IS NOT NULL
              AND (:country IS NULL OR ds.country_iso3 = :country)
            ORDER BY ds.country_iso3, dp.date ASC
            """,
            {"country": country_iso3},
        )

        # Tertiary enrollment (brain gain proxy: higher enrollment = gain channel active)
        tertiary_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SE.TER.ENRR'
              AND dp.value IS NOT NULL
              AND (:country IS NULL OR ds.country_iso3 = :country)
            ORDER BY ds.country_iso3, dp.date ASC
            """,
            {"country": country_iso3},
        )

        # Skilled emigration rate from custom source
        skilled_emig_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value, dp.date
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.source IN ('skilled_emigration', 'migration_development')
              AND ds.series_id LIKE '%skilled_emigration%'
              AND dp.value IS NOT NULL
              AND (:country IS NULL OR ds.country_iso3 = :country)
            ORDER BY dp.date DESC
            """,
            {"country": country_iso3},
        )

        # Build per-country dictionaries
        def build_latest(rows, value_key="value") -> dict[str, float]:
            out: dict[str, float] = {}
            for r in rows:
                iso = r["country_iso3"]
                if iso not in out:
                    out[iso] = float(r[value_key])
            return out

        def build_series(rows) -> dict[str, list[tuple[str, float]]]:
            out: dict[str, list[tuple[str, float]]] = {}
            for r in rows:
                out.setdefault(r["country_iso3"], []).append((r["date"], float(r["value"])))
            return out

        remit_latest = build_latest(list(reversed(sorted(remit_rows, key=lambda r: r["date"]))))
        fdi_latest = build_latest(list(reversed(sorted(fdi_rows, key=lambda r: r["date"]))))
        tertiary_latest = build_latest(list(reversed(sorted(tertiary_rows, key=lambda r: r["date"]))))
        skilled_emig_map: dict[str, float] = {}
        for r in skilled_emig_rows:
            if r["country_iso3"] not in skilled_emig_map:
                skilled_emig_map[r["country_iso3"]] = float(r["value"])

        remit_series = build_series(remit_rows)
        net_migr_series = build_series(net_migr_rows)

        # Remittance analysis for target country
        remit_analysis = None
        target_remit = remit_latest.get(country_iso3) if country_iso3 else None
        if country_iso3 and country_iso3 in remit_series and len(remit_series[country_iso3]) >= 5:
            rs = sorted(remit_series[country_iso3], key=lambda x: x[0])
            vals = np.array([v for _, v in rs])
            # Countercyclicality: check if remittances are counter-cyclical
            t_idx = np.arange(len(vals), dtype=float)
            slope, _, r_val, p_val, _ = sp_stats.linregress(t_idx, vals)

            remit_analysis = {
                "current_pct_gdp": round(float(vals[-1]), 3),
                "trend_slope_per_yr": round(float(slope), 4),
                "trend_pval": round(float(p_val), 4),
                "mean_pct_gdp": round(float(np.mean(vals)), 3),
                "n_years": len(vals),
                "classification": (
                    "remittance-dependent" if float(vals[-1]) > 10
                    else "significant remittance receiver" if float(vals[-1]) > 5
                    else "modest remittance flows"
                ),
            }

        # Net migration trend
        migration_trend = None
        if country_iso3 and country_iso3 in net_migr_series and len(net_migr_series[country_iso3]) >= 3:
            nm = sorted(net_migr_series[country_iso3], key=lambda x: x[0])
            nm_vals = np.array([v for _, v in nm])
            migration_trend = {
                "current": round(float(nm_vals[-1]), 0),
                "mean": round(float(np.mean(nm_vals)), 0),
                "direction": "net emigration" if float(nm_vals[-1]) < 0 else "net immigration",
            }

        # Diaspora-FDI correlation (cross-country)
        diaspora_fdi_corr = None
        common_isos = sorted(set(remit_latest.keys()) & set(fdi_latest.keys()))
        if len(common_isos) >= 20:
            remit_arr = np.array([remit_latest[c] for c in common_isos])
            fdi_arr = np.array([fdi_latest[c] for c in common_isos])
            corr, pval = sp_stats.pearsonr(remit_arr, fdi_arr)
            diaspora_fdi_corr = {
                "pearson_r": round(float(corr), 4),
                "pval": round(float(pval), 4),
                "significant": float(pval) < 0.10,
                "n_countries": len(common_isos),
                "interpretation": (
                    "remittance-receiving countries attract more FDI (diaspora channel)"
                    if float(corr) > 0.1 and float(pval) < 0.10
                    else "no significant diaspora-FDI link in cross-country data"
                ),
            }

        # Brain drain assessment
        skilled_emig = skilled_emig_map.get(country_iso3) if country_iso3 else None
        tertiary_enrol = tertiary_latest.get(country_iso3) if country_iso3 else None

        brain_drain_assessment = None
        if skilled_emig is not None:
            # Beine-Docquier-Rapoport: brain gain channel requires
            # return probability > 0 and education elasticity > emigration rate
            brain_gain_possible = tertiary_enrol is not None and tertiary_enrol > 20.0
            brain_drain_assessment = {
                "skilled_emigration_rate_pct": round(float(skilled_emig), 2),
                "tertiary_enrollment_pct": round(float(tertiary_enrol), 2) if tertiary_enrol is not None else None,
                "brain_gain_channel_likely": brain_gain_possible and skilled_emig < 30.0,
                "interpretation": (
                    "severe brain drain, net human capital loss"
                    if skilled_emig > 40
                    else "moderate brain drain" if skilled_emig > 20
                    else "modest skilled emigration, brain gain channel plausible"
                ),
            }

        # Score construction
        score = 40.0  # neutral baseline

        # Remittances (high remittances = stabilizing but dependency risk)
        if target_remit is not None:
            if target_remit > 15:
                score += 15.0  # high dependency
            elif target_remit > 5:
                score -= 5.0   # meaningful inflows, stabilizing
            elif target_remit < 1:
                score += 5.0   # low remittances, missing development channel

        # Brain drain
        if skilled_emig is not None:
            if skilled_emig > 40:
                score += 25.0
            elif skilled_emig > 20:
                score += 12.0
            elif skilled_emig < 10:
                score -= 8.0

        # Brain gain channel mitigates drain
        if brain_drain_assessment and brain_drain_assessment["brain_gain_channel_likely"]:
            score -= 10.0

        score = max(0.0, min(100.0, score))

        results: dict = {
            "country_iso3": country_iso3,
            "remittances": remit_analysis or {
                "current_pct_gdp": round(target_remit, 3) if target_remit is not None else None,
            },
            "net_migration_trend": migration_trend,
            "brain_drain": brain_drain_assessment,
            "diaspora_fdi_channel": diaspora_fdi_corr,
            "target_fdi_pct_gdp": round(float(fdi_latest[country_iso3]), 3)
                if country_iso3 and country_iso3 in fdi_latest else None,
        }

        return {"score": round(score, 2), "results": results}
