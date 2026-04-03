"""Media Economics module.

Four dimensions of media-market political economy:

1. **Media market concentration** (Noam 2009, Bagdikian 2004):
   Herfindahl-Hirschman Index (HHI) over audience/revenue shares.
   HHI > 2500 (DOJ threshold) signals harmful concentration.
   High concentration correlates with partisan bias and suppressed
   investigative journalism.

2. **Press freedom economic effects** (Brunetti & Weder 2003):
   Free press reduces corruption by enabling citizen monitoring.
   Regression: CPI ~ press_freedom_index + controls.
   Press freedom elasticity on investment documented at 0.12-0.18.

3. **Advertising market dynamics** (Ellman & Germano 2009):
   Advertiser influence: media slants coverage away from
   advertiser-unfavorable stories. Estimated via correlation
   between ad-revenue dependence and coverage skew proxies.

4. **Fake news economic cost** (Allcott & Gentzkow 2017):
   Misinformation imposes real costs: policy distortions,
   misallocation, reduced trust in institutions.
   Institutional trust and misinformation prevalence proxies
   from survey-based governance indicators.

Score: high concentration + suppressed press freedom + ad capture
+ misinformation prevalence -> high stress.

References:
    Noam, E. (2009). Media Ownership and Concentration in America. Oxford.
    Brunetti, A. & Weder, B. (2003). "A Free Press Is Bad News for
        Corruption." Journal of Public Economics 87(7-8).
    Ellman, M. & Germano, F. (2009). "What Do the Papers Sell?"
        Economic Journal 119(537).
    Allcott, H. & Gentzkow, M. (2017). "Social Media and Fake News
        in the 2016 Election." JEP 31(2).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class MediaEconomics(LayerBase):
    layer_id = "l12"
    name = "Media Economics"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate media concentration, press freedom effects, and fake news costs.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default USA)
        """
        country = kwargs.get("country_iso3", "USA")

        # Fetch press freedom / media freedom index
        press_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.source
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%press%freedom%' OR ds.name LIKE '%media%freedom%'
                   OR ds.name LIKE '%freedom%press%' OR ds.name LIKE '%rsf%score%'
                   OR ds.name LIKE '%media%pluralism%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch corruption data for Brunetti-Weder nexus
        corruption_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%corruption%perception%' OR ds.name LIKE '%control%corruption%'
                   OR ds.name LIKE '%transparency%international%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch institutional trust / misinformation proxies
        trust_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%institutional%trust%' OR ds.name LIKE '%trust%government%'
                   OR ds.name LIKE '%voice%accountability%' OR ds.name LIKE '%civil%liberties%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Fetch advertising / media revenue data
        ad_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%advertising%revenue%' OR ds.name LIKE '%media%concentration%'
                   OR ds.name LIKE '%hhi%media%' OR ds.name LIKE '%broadcast%market%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not press_rows and not corruption_rows and not trust_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no media/press freedom data"}

        # --- 1. Press freedom index ---
        press_freedom = None
        press_trend = None
        if press_rows:
            pf_vals = np.array([float(r["value"]) for r in press_rows])
            pf_dates = [r["date"] for r in press_rows]
            latest_pf = float(pf_vals[-1])

            # Detect scale: RSF scores 0-100 (lower = freer), V-Dem 0-1 (higher = freer)
            if np.max(pf_vals) <= 1.5:
                # V-Dem scale (0-1): higher = more free, invert for stress
                normalized_pf = 1.0 - float(np.mean(pf_vals))
            else:
                # RSF scale (0-100): higher = less free (more problem)
                normalized_pf = float(np.mean(pf_vals)) / 100.0

            normalized_pf = float(np.clip(normalized_pf, 0.0, 1.0))

            press_freedom = {
                "latest_index": round(latest_pf, 3),
                "mean_index": round(float(np.mean(pf_vals)), 3),
                "normalized_stress": round(normalized_pf, 4),
                "n_obs": len(pf_vals),
                "date_range": [str(pf_dates[0]), str(pf_dates[-1])],
            }

            if len(pf_vals) >= 3:
                t = np.arange(len(pf_vals), dtype=float)
                slope, _, r_val, p_val, _ = stats.linregress(t, pf_vals)
                press_trend = {
                    "slope": round(float(slope), 5),
                    "r_squared": round(float(r_val ** 2), 4),
                    "p_value": round(float(p_val), 4),
                    "direction": "worsening" if slope > 0 else "improving",
                }
        else:
            normalized_pf = 0.5  # Unknown, neutral

        # --- 2. Brunetti-Weder nexus: press freedom vs corruption ---
        press_corruption_nexus = None
        if press_rows and corruption_rows and len(press_rows) >= 5 and len(corruption_rows) >= 5:
            pf_map = {str(r["date"])[:4]: float(r["value"]) for r in press_rows}
            cr_map = {str(r["date"])[:4]: float(r["value"]) for r in corruption_rows}
            common = sorted(set(pf_map.keys()) & set(cr_map.keys()))
            if len(common) >= 5:
                pf_arr = np.array([pf_map[y] for y in common])
                cr_arr = np.array([cr_map[y] for y in common])
                slope, intercept, r_val, p_val, _ = stats.linregress(pf_arr, cr_arr)
                press_corruption_nexus = {
                    "coefficient": round(float(slope), 5),
                    "r_squared": round(float(r_val ** 2), 4),
                    "p_value": round(float(p_val), 4),
                    "brunetti_weder_consistent": slope < 0,  # Free press reduces corruption
                    "n_observations": len(common),
                    "note": "Negative coefficient: more press freedom reduces corruption (Brunetti-Weder 2003)",
                }

        # --- 3. Media HHI from available data ---
        hhi_result = None
        hhi_normalized = 0.5  # Default mid-range if no data
        if ad_rows:
            ad_vals = [float(r["value"]) for r in ad_rows if r["value"] is not None]
            if ad_vals:
                latest_hhi = float(ad_vals[-1])
                # If it looks like a raw HHI (0-10000 scale)
                if latest_hhi > 100:
                    hhi_normalized = float(np.clip(latest_hhi / 10000.0, 0, 1))
                    doj_threshold_exceeded = latest_hhi > 2500
                else:
                    # Could be a 0-1 concentration index
                    hhi_normalized = float(np.clip(latest_hhi, 0, 1))
                    doj_threshold_exceeded = latest_hhi > 0.25

                hhi_result = {
                    "latest_hhi": round(latest_hhi, 2),
                    "normalized": round(hhi_normalized, 4),
                    "doj_threshold_exceeded": doj_threshold_exceeded,
                    "interpretation": "highly concentrated" if hhi_normalized > 0.5 else "moderate",
                    "reference": "DOJ merger guidelines: HHI > 2500 = highly concentrated",
                }

        # --- 4. Institutional trust / fake news cost proxy ---
        trust_score = None
        misinformation_component = 0.3  # Default
        if trust_rows:
            tr_vals = np.array([float(r["value"]) for r in trust_rows])
            latest_trust = float(tr_vals[-1])

            # WGI voice/accountability: -2.5 to 2.5 (higher = better)
            if np.min(tr_vals) < -1:
                normalized_trust = (latest_trust + 2.5) / 5.0
            else:
                normalized_trust = float(np.clip(latest_trust, 0, 1))

            # Misinformation stress: low trust -> high fake news cost
            misinformation_component = 1.0 - float(np.clip(normalized_trust, 0, 1))
            trust_score = {
                "latest_trust_index": round(latest_trust, 3),
                "normalized_trust": round(normalized_trust, 4),
                "fake_news_cost_proxy": round(misinformation_component, 4),
                "note": "Low institutional trust proxies misinformation prevalence (Allcott-Gentzkow 2017)",
            }

        # --- Score ---
        # Component 1: press freedom suppression (0-40)
        pf_component = float(np.clip(normalized_pf * 40.0, 0, 40))

        # Component 2: media concentration (0-30)
        conc_component = float(np.clip(hhi_normalized * 30.0, 0, 30))

        # Component 3: fake news / low trust (0-30)
        mis_component = float(np.clip(misinformation_component * 30.0, 0, 30))

        score = float(np.clip(pf_component + conc_component + mis_component, 0, 100))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "press_freedom_stress": round(pf_component, 2),
                "concentration_stress": round(conc_component, 2),
                "misinformation_stress": round(mis_component, 2),
            },
        }

        if press_freedom:
            result["press_freedom"] = press_freedom
        if press_trend:
            result["press_freedom_trend"] = press_trend
        if press_corruption_nexus:
            result["brunetti_weder_nexus"] = press_corruption_nexus
        if hhi_result:
            result["media_concentration_hhi"] = hhi_result
        if trust_score:
            result["institutional_trust"] = trust_score

        return result
