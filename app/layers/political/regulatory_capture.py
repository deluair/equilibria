"""Regulatory Capture module.

Four dimensions based on Stigler-Peltzman-Becker regulatory economics:

1. **Stigler capture theory indicators** (Stigler 1971):
   Industries capture their regulators over time. Indicators: regulatory
   leniency vs industry size, enforcement actions per firm, penalty rates.
   Laffont-Tirole (1991): incomplete information allows regulated firms
   to exploit informational advantage over regulators.

2. **Revolving door frequency** (Blanes i Vidal et al. 2012):
   Rate at which regulators move to regulated industries (and vice versa).
   Blanes i Vidal: former congressional staffers see 24% earnings premium
   when their ex-boss holds committee power. Estimated from governance
   indicators and sector-specific capture proxies.

3. **Industry concentration and regulation** (Peltzman 1976):
   Higher industry concentration -> more effective lobbying -> regulatory
   capture. Herfindahl index of regulated sectors. Peltzman: regulation
   benefits both producers and certain consumer groups.

4. **Consumer welfare loss** (deadweight loss estimation):
   Price wedge between competitive benchmark and regulated/monopoly price.
   Harberger triangle: DWL = 0.5 * (P - MC) * (Q_c - Q_m).
   Estimated from markup data and price indices.

Score: strong capture indicators + high revolving door + concentrated
industries + large consumer welfare loss -> high stress.

References:
    Stigler, G. (1971). "The Theory of Economic Regulation." Bell JE 2(1).
    Peltzman, S. (1976). "Toward a More General Theory of Regulation."
        JLE 19(2).
    Laffont, J.-J. & Tirole, J. (1991). "The Politics of Government
        Decision-Making." QJE 106(4).
    Blanes i Vidal, J., Draca, M. & Fons-Rosen, C. (2012). "Revolving
        Door Lobbyists." AER 102(7).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class RegulatoryCapture(LayerBase):
    layer_id = "l12"
    name = "Regulatory Capture"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate Stigler-Peltzman regulatory capture indicators.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default USA)
        """
        country = kwargs.get("country_iso3", "USA")

        # Regulatory quality / capture proxy
        reg_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%regulatory%quality%' OR ds.name LIKE '%regulatory%capture%'
                   OR ds.name LIKE '%regulatory%effectiveness%' OR ds.name LIKE '%wgi%regulatory%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Industry concentration (HHI of major regulated sectors)
        concentration_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%market%concentration%' OR ds.name LIKE '%herfindahl%'
                   OR ds.name LIKE '%industry%concentration%' OR ds.name LIKE '%market%power%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Price markup data (for DWL estimation)
        markup_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('fred', 'wdi', 'bls')
              AND (ds.name LIKE '%price%markup%' OR ds.name LIKE '%profit%margin%'
                   OR ds.name LIKE '%lerner%index%' OR ds.name LIKE '%price%cost%margin%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Revolving door / corruption-related governance
        governance_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%revolving%door%' OR ds.name LIKE '%control%corruption%'
                   OR ds.name LIKE '%regulatory%burden%' OR ds.name LIKE '%ease%doing%business%')
            ORDER BY dp.date
            """,
            (country,),
        )

        all_empty = not any([reg_rows, concentration_rows, markup_rows, governance_rows])
        if all_empty:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no regulatory capture data"}

        # --- 1. Stigler capture indicators (regulatory quality inversion) ---
        capture_index = None
        capture_stress = 0.5
        if reg_rows:
            rv = np.array([float(r["value"]) for r in reg_rows])
            reg_dates = [r["date"] for r in reg_rows]
            latest_reg = float(rv[-1])

            # WGI regulatory quality: -2.5 to 2.5 (higher = better quality / less capture)
            if np.min(rv) < -1:
                normalized = (latest_reg + 2.5) / 5.0
            elif np.max(rv) <= 1.5:
                normalized = float(latest_reg)
            else:
                # 0-100 scale (higher = better)
                normalized = latest_reg / 100.0

            # Capture stress = inverse of quality
            capture_stress = 1.0 - float(np.clip(normalized, 0, 1))

            # Trend
            trend = None
            if len(rv) >= 3:
                t = np.arange(len(rv), dtype=float)
                slope, _, r_val, p_val, _ = stats.linregress(t, rv)
                trend = {
                    "slope": round(float(slope), 5),
                    "direction": "improving" if slope > 0 else "worsening",
                    "r_squared": round(float(r_val ** 2), 4),
                    "p_value": round(float(p_val), 4),
                }

            capture_index = {
                "latest_regulatory_quality": round(latest_reg, 3),
                "normalized_0_1": round(float(np.clip(normalized, 0, 1)), 4),
                "capture_stress": round(capture_stress, 4),
                "stigler_capture_risk": "high" if capture_stress > 0.6 else "moderate" if capture_stress > 0.35 else "low",
                "n_obs": len(rv),
                "date_range": [str(reg_dates[0]), str(reg_dates[-1])],
                "reference": "Stigler 1971; Laffont & Tirole 1991; WGI Regulatory Quality",
            }
            if trend:
                capture_index["trend"] = trend

        # --- 2. Industry concentration ---
        concentration_analysis = None
        concentration_stress = 0.5
        if concentration_rows:
            cv = np.array([float(r["value"]) for r in concentration_rows])
            conc_dates = [r["date"] for r in concentration_rows]
            latest_conc = float(cv[-1])

            # Normalize HHI: 0-10000 or 0-1
            if latest_conc > 100:
                conc_normalized = float(np.clip(latest_conc / 10000.0, 0, 1))
                doj_threshold = latest_conc > 2500
            else:
                conc_normalized = float(np.clip(latest_conc, 0, 1))
                doj_threshold = latest_conc > 0.25

            concentration_stress = conc_normalized

            concentration_analysis = {
                "latest_concentration": round(latest_conc, 3),
                "normalized_hhi": round(conc_normalized, 4),
                "doj_highly_concentrated": doj_threshold,
                "peltzman_capture_risk": "high" if conc_normalized > 0.5 else "moderate" if conc_normalized > 0.25 else "low",
                "n_obs": len(cv),
                "date_range": [str(conc_dates[0]), str(conc_dates[-1])],
                "reference": "Peltzman 1976: concentration enables effective lobbying",
            }

        # --- 3. Consumer welfare loss (Harberger DWL) ---
        dwl_analysis = None
        dwl_stress = 0.3
        if markup_rows:
            mv = np.array([float(r["value"]) for r in markup_rows])
            markup_dates = [r["date"] for r in markup_rows]
            latest_markup = float(mv[-1])

            # Markup as Lerner index: (P-MC)/P or profit margin as proxy
            # Normalize: 0-1 range
            if latest_markup > 1:
                # Percentage markup (e.g., 25 = 25%)
                lerner = float(np.clip(latest_markup / 100.0, 0, 1))
            else:
                lerner = float(np.clip(latest_markup, 0, 1))

            # Harberger DWL = 0.5 * lerner^2 / elasticity
            # Assume unit demand elasticity for normalization
            dwl_pct_gdp = 0.5 * (lerner ** 2)  # As fraction of revenue
            dwl_stress = float(np.clip(lerner, 0, 1))

            dwl_analysis = {
                "latest_markup": round(latest_markup, 4),
                "lerner_index_proxy": round(lerner, 4),
                "harberger_dwl_pct_revenue": round(dwl_pct_gdp * 100, 2),
                "consumer_welfare_loss_severity": "high" if lerner > 0.3 else "moderate" if lerner > 0.1 else "low",
                "n_obs": len(mv),
                "date_range": [str(markup_dates[0]), str(markup_dates[-1])],
                "reference": "Harberger 1954: DWL = 0.5*(P-MC)*(Qc-Qm); Lerner index",
            }

        # --- 4. Revolving door proxy from governance ---
        revolving_analysis = None
        revolving_stress = 0.5
        if governance_rows:
            gv = np.array([float(r["value"]) for r in governance_rows])
            gov_dates = [r["date"] for r in governance_rows]
            latest_gov = float(gv[-1])

            # Corruption control (WGI): -2.5 to 2.5 (higher = less corruption = less revolving door)
            if np.min(gv) < -1:
                rev_normalized = (latest_gov + 2.5) / 5.0
            elif np.max(gv) <= 1.5:
                rev_normalized = float(latest_gov)
            else:
                rev_normalized = latest_gov / 100.0

            revolving_stress = 1.0 - float(np.clip(rev_normalized, 0, 1))

            revolving_analysis = {
                "latest_governance_index": round(latest_gov, 3),
                "revolving_door_stress": round(revolving_stress, 4),
                "n_obs": len(gv),
                "date_range": [str(gov_dates[0]), str(gov_dates[-1])],
                "reference": "Blanes i Vidal et al. 2012: revolving door 24% earnings premium",
            }

        # --- Score ---
        # Weights: capture indicators 30, concentration 25, DWL 25, revolving door 20
        score = float(np.clip(
            capture_stress * 30.0
            + concentration_stress * 25.0
            + dwl_stress * 25.0
            + revolving_stress * 20.0,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "stigler_capture": round(capture_stress * 30.0, 2),
                "industry_concentration": round(concentration_stress * 25.0, 2),
                "consumer_welfare_loss": round(dwl_stress * 25.0, 2),
                "revolving_door": round(revolving_stress * 20.0, 2),
            },
        }

        if capture_index:
            result["stigler_capture_indicators"] = capture_index
        if concentration_analysis:
            result["industry_concentration"] = concentration_analysis
        if dwl_analysis:
            result["consumer_welfare_loss"] = dwl_analysis
        if revolving_analysis:
            result["revolving_door"] = revolving_analysis

        return result
