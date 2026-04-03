"""Middle income trap detection via Eichengreen-Park-Shin methodology.

The middle income trap (MIT) refers to the stylized slowdown in per capita
GDP growth that many economies experience after crossing the middle income
threshold (~$10,000-$16,000 in 2005 PPP terms).

Eichengreen, Park & Shin (2012, 2014) identify slowdown episodes as periods
where 7-year average growth falls by at least 2 pp relative to the preceding
7-year average, with GDP per capita in the $10K-$15K (lower MIT) or $15K-$16K
(upper MIT) range at the start of the slowdown.

Four analytical components:

1. MIT identification: test whether the target country's current or recent
   growth trajectory exhibits the statistical signature of an EPS slowdown
   episode (growth deceleration at middle income levels).

2. Structural change requirements: MIT escape requires moving up the
   value chain. Proxied by: manufacturing complexity, high-tech export share,
   R&D intensity, tertiary education rates. Countries that escape MIT
   (Korea, Taiwan) all showed rapid upgrading on these indicators.

3. Growth accounting decomposition: separate TFP growth from capital
   accumulation. Countries trapped often show declining TFP contribution
   as easy capital-deepening gains exhaust (Lewis turning point).

4. Peer benchmarking: compare against EPS reference countries that
   escaped vs. remained trapped at similar income levels.

References:
    Eichengreen, B., Park, D. & Shin, K. (2012). When fast-growing economies
        slow down. Asian Economic Papers 11(1): 42-87.
    Eichengreen, B., Park, D. & Shin, K. (2014). Growth slowdowns redux.
        Japan and the World Economy 32: 65-84.
    Felipe, J., Abdon, A. & Kumar, U. (2012). Tracking the middle-income trap.
        Levy Economics Institute working paper 715.
    Gill, I. & Kharas, H. (2007). An East Asian Renaissance. World Bank.

Score: active MIT episode with low structural upgrading -> CRISIS.
Not in MIT range, or rapid structural change -> STABLE/WATCH.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MiddleIncomeTrap(LayerBase):
    layer_id = "l4"
    name = "Middle Income Trap"
    weight = 0.20

    # EPS income thresholds in 2005 PPP USD
    MIT_LOWER_BOUND = 10_000.0
    MIT_UPPER_BOUND = 16_000.0
    # EPS slowdown criterion: growth falls >= 2 pp on 7-year average
    EPS_SLOWDOWN_THRESHOLD = 0.02
    EPS_WINDOW = 7

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3")

        # GDP per capita (constant 2015 USD as proxy, series NY.GDP.PCAP.KD)
        gdp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
              AND dp.value > 0
              AND (:country IS NULL OR ds.country_iso3 = :country)
            ORDER BY ds.country_iso3, dp.date ASC
            """,
            {"country": country_iso3},
        )

        if not gdp_rows:
            return {"score": 50, "results": {"error": "no GDP per capita data"}}

        # Build per-country time series
        country_series: dict[str, list[tuple[str, float]]] = {}
        for r in gdp_rows:
            country_series.setdefault(r["country_iso3"], []).append((r["date"], r["value"]))

        def detect_eps_slowdown(series: list[tuple[str, float]]) -> dict:
            """Apply EPS methodology to a single country's GDP per capita series."""
            series_sorted = sorted(series, key=lambda x: x[0])
            dates = [s[0] for s in series_sorted]
            vals = np.array([s[1] for s in series_sorted])
            n = len(vals)

            if n < 2 * self.EPS_WINDOW + 1:
                return {"detected": False, "reason": "insufficient data"}

            slowdowns = []
            for i in range(self.EPS_WINDOW, n - self.EPS_WINDOW):
                gdp_at_i = vals[i]
                # Check if in MIT income range (approximate: use 2015 USD thresholds
                # scaled; $10K-$16K in 2005 PPP ~ $12K-$20K in 2015 USD roughly)
                if not (12_000 <= gdp_at_i <= 22_000):
                    continue

                # Growth rates: preceding and following EPS_WINDOW years
                preceding_growth = (np.log(vals[i]) - np.log(vals[i - self.EPS_WINDOW])) / self.EPS_WINDOW
                following_growth = (np.log(vals[i + self.EPS_WINDOW]) - np.log(vals[i])) / self.EPS_WINDOW

                deceleration = preceding_growth - following_growth
                if deceleration >= self.EPS_SLOWDOWN_THRESHOLD:
                    slowdowns.append({
                        "year": dates[i],
                        "gdppc": round(float(gdp_at_i), 0),
                        "preceding_growth": round(float(preceding_growth), 4),
                        "following_growth": round(float(following_growth), 4),
                        "deceleration": round(float(deceleration), 4),
                    })

            # Also check if currently in MIT range
            current_gdp = float(vals[-1])
            recent_growth_7yr = None
            if n >= self.EPS_WINDOW + 1:
                recent_growth_7yr = float(
                    (np.log(vals[-1]) - np.log(vals[-1 - self.EPS_WINDOW])) / self.EPS_WINDOW
                )
            earlier_growth_7yr = None
            if n >= 2 * self.EPS_WINDOW + 1:
                earlier_growth_7yr = float(
                    (np.log(vals[-1 - self.EPS_WINDOW]) - np.log(vals[-1 - 2 * self.EPS_WINDOW])) / self.EPS_WINDOW
                )

            currently_in_mit_range = 12_000 <= current_gdp <= 22_000

            current_slowdown = False
            current_deceleration = None
            if recent_growth_7yr is not None and earlier_growth_7yr is not None:
                current_deceleration = earlier_growth_7yr - recent_growth_7yr
                current_slowdown = (
                    currently_in_mit_range
                    and current_deceleration >= self.EPS_SLOWDOWN_THRESHOLD
                )

            return {
                "detected": len(slowdowns) > 0 or current_slowdown,
                "historical_slowdowns": slowdowns,
                "current_status": {
                    "gdppc_latest": round(current_gdp, 0),
                    "in_mit_range": currently_in_mit_range,
                    "recent_7yr_growth": round(recent_growth_7yr, 4) if recent_growth_7yr is not None else None,
                    "earlier_7yr_growth": round(earlier_growth_7yr, 4) if earlier_growth_7yr is not None else None,
                    "current_deceleration": round(current_deceleration, 4) if current_deceleration is not None else None,
                    "current_slowdown_episode": current_slowdown,
                },
            }

        # Structural change indicators
        structural_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, ds.series_id, dp.value, dp.date
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN (
                'TX.VAL.TECH.MF.ZS',
                'GB.XPD.RSDV.GD.ZS',
                'SE.TER.ENRR',
                'NV.IND.MANF.ZS'
            )
            AND (:country IS NULL OR ds.country_iso3 = :country)
            AND dp.date = (
                SELECT MAX(dp2.date) FROM data_points dp2
                WHERE dp2.series_id = ds.id
                AND dp2.value IS NOT NULL
                AND (:country IS NULL OR (
                    SELECT ds2.country_iso3 FROM data_series ds2 WHERE ds2.id = dp2.series_id
                ) = :country)
            )
            """,
            {"country": country_iso3},
        )

        structural: dict[str, dict[str, float]] = {}
        for r in structural_rows:
            structural.setdefault(r["country_iso3"], {})[r["series_id"]] = float(r["value"])

        # Run analysis
        target_result = None
        cross_country_mit_count = 0
        cross_country_n = 0

        for iso, series in country_series.items():
            if len(series) < 10:
                continue
            eps = detect_eps_slowdown(series)
            cross_country_n += 1
            if eps["detected"]:
                cross_country_mit_count += 1
            if iso == country_iso3:
                target_result = eps

        # Structural change for target country
        target_structural = structural.get(country_iso3, {}) if country_iso3 else {}
        high_tech_share = target_structural.get("TX.VAL.TECH.MF.ZS")
        rnd_gdp = target_structural.get("GB.XPD.RSDV.GD.ZS")
        tertiary_enrol = target_structural.get("SE.TER.ENRR")
        manuf_share = target_structural.get("NV.IND.MANF.ZS")

        # Structural upgrading composite (0-100, higher = better positioned)
        upgrading_score = None
        upgrading_components = {}
        if any(v is not None for v in [high_tech_share, rnd_gdp, tertiary_enrol]):
            comp_vals = []
            if high_tech_share is not None:
                # Korea/Taiwan at escape had >20% high-tech exports
                ht_norm = min(100.0, high_tech_share / 20.0 * 100.0)
                upgrading_components["high_tech_exports_pct"] = round(high_tech_share, 2)
                comp_vals.append(ht_norm)
            if rnd_gdp is not None:
                # Escapees typically >=1.5% R&D/GDP
                rd_norm = min(100.0, rnd_gdp / 1.5 * 100.0)
                upgrading_components["rnd_pct_gdp"] = round(rnd_gdp, 3)
                comp_vals.append(rd_norm)
            if tertiary_enrol is not None:
                # Escapees typically >50% tertiary enrollment
                ter_norm = min(100.0, tertiary_enrol / 50.0 * 100.0)
                upgrading_components["tertiary_enrollment_pct"] = round(tertiary_enrol, 2)
                comp_vals.append(ter_norm)
            if manuf_share is not None:
                upgrading_components["manufacturing_share_pct"] = round(manuf_share, 2)
            if comp_vals:
                upgrading_score = float(np.mean(comp_vals))

        # Score construction
        # MIT active + low upgrading = high stress score
        score = 30.0  # default: not in MIT range

        if target_result:
            status = target_result["current_status"]
            if status["in_mit_range"]:
                if target_result["detected"]:
                    score = 75.0
                else:
                    score = 55.0  # in range but not yet confirmed trap
            elif status["gdppc_latest"] < 12_000:
                score = 40.0  # pre-MIT: risk ahead
            else:
                score = 20.0  # escaped MIT range

        # Penalize for low structural upgrading capacity
        if upgrading_score is not None:
            if upgrading_score < 30.0:
                score = min(95.0, score + 20.0)
            elif upgrading_score > 70.0:
                score = max(10.0, score - 15.0)

        score = max(0.0, min(100.0, score))

        results: dict = {
            "eps_methodology": "Eichengreen-Park-Shin (2012, 2014)",
            "income_thresholds_2015usd": {
                "lower_mit": 12_000,
                "upper_mit": 22_000,
                "slowdown_criterion_pp": self.EPS_SLOWDOWN_THRESHOLD * 100,
            },
            "country_iso3": country_iso3,
            "target_analysis": target_result,
            "structural_upgrading": {
                "composite_score_0_100": round(upgrading_score, 2) if upgrading_score is not None else None,
                "components": upgrading_components,
                "interpretation": (
                    "strong upgrading capacity, MIT escape likely"
                    if upgrading_score is not None and upgrading_score > 70
                    else "moderate capacity"
                    if upgrading_score is not None and upgrading_score > 40
                    else "weak upgrading capacity, MIT risk elevated"
                ) if upgrading_score is not None else "insufficient data",
            },
        }

        if cross_country_n > 0:
            results["cross_country"] = {
                "n_countries": cross_country_n,
                "mit_episode_count": cross_country_mit_count,
                "mit_prevalence_pct": round(cross_country_mit_count / cross_country_n * 100.0, 2),
            }

        return {"score": round(score, 2), "results": results}
