"""Privacy Valuation module.

Four dimensions of the economics of privacy and data markets:

1. **WTP for privacy** (conjoint analysis, Acquisti et al. 2016):
   Willingness-to-pay for privacy protection. Meta-analyses find large
   inconsistencies (privacy paradox): stated WTP >> revealed WTP.
   Estimated from consumer sentiment, digital adoption data, and
   cybersecurity spending as revealed preference proxies.

2. **Data market valuation** (Posner 1981, Jones & Tonetti 2020):
   Value of consumer data to platforms. Jones-Tonetti: data is
   non-rival, creates monopoly risks, and optimal policy may involve
   consumer data ownership. Estimated from digital economy share of GDP,
   platform market capitalization proxies, and intangible asset ratios.

3. **GDPR compliance costs** (Goldberg et al. 2019):
   GDPR reduced EU website traffic 7-10% and reduced venture capital
   investment. Direct compliance costs: IT, legal, DPO salaries.
   Estimated as % of revenue for regulated firms (typically 1-3%).

4. **Surveillance capitalism externalities** (Zuboff 2019, Acemoglu et al. 2022):
   Behavioral surplus extraction: platforms predict and modify behavior.
   Acemoglu et al. 2022: AI and surveillance create negative externalities
   on labor, democracy, and autonomy. Proxied by digital market concentration,
   data broker prevalence, and targeted advertising intensity.

Score: low revealed WTP for privacy (privacy paradox) + high data market
concentration + GDPR non-compliance + surveillance economy size -> high stress.

References:
    Acquisti, A., Taylor, C. & Wagman, L. (2016). "The Economics of Privacy."
        JEL 54(2).
    Jones, C. & Tonetti, C. (2020). "Nonrivalry and the Economics of Data."
        AER 110(9).
    Goldberg, S., Johnson, G. & Shriver, S. (2019). "Regulating Privacy
        Online." Working paper.
    Acemoglu, D. et al. (2022). "Too Much Data: Prices and Inefficiencies
        in Data Markets." AEJ: Microeconomics 14(4).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class PrivacyValuation(LayerBase):
    layer_id = "l13"
    name = "Privacy Valuation"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate privacy valuation, data markets, and surveillance externalities.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default USA)
        """
        country = kwargs.get("country_iso3", "USA")

        # Digital economy / ICT data
        digital_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%ict%value%added%' OR ds.name LIKE '%digital%economy%'
                   OR ds.name LIKE '%internet%penetration%' OR ds.name LIKE '%broadband%'
                   OR ds.name LIKE '%individuals%using%internet%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Cybersecurity spending / data protection expenditure
        cyber_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%cybersecurity%' OR ds.name LIKE '%data%protection%spending%'
                   OR ds.name LIKE '%information%security%' OR ds.name LIKE '%privacy%compliance%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Intangible asset / intellectual property data (data market proxy)
        intangible_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('fred', 'wdi', 'bls')
              AND (ds.name LIKE '%intellectual%property%' OR ds.name LIKE '%intangible%asset%'
                   OR ds.name LIKE '%r%d%expenditure%gdp%' OR ds.name LIKE '%software%investment%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Digital market concentration (advertising / platform revenue)
        concentration_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%digital%market%concentration%' OR ds.name LIKE '%platform%market%share%'
                   OR ds.name LIKE '%digital%advertising%market%' OR ds.name LIKE '%market%power%digital%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not digital_rows and not intangible_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no digital economy/privacy data"}

        # --- 1. Internet penetration / digital adoption (privacy paradox proxy) ---
        digital_analysis = None
        digital_stress = 0.4
        if digital_rows:
            dv_map: dict[str, list] = {}
            for r in digital_rows:
                dv_map.setdefault(r["series_id"], []).append((r["date"], float(r["value"])))

            primary_sid = max(dv_map, key=lambda s: len(dv_map[s]))
            primary = sorted(dv_map[primary_sid], key=lambda x: x[0])
            dates = [d for d, _ in primary]
            vals = np.array([v for _, v in primary])
            latest_val = float(vals[-1])

            # Internet penetration: 0-100%
            if np.max(vals) > 1:
                internet_pct = latest_val
            else:
                internet_pct = latest_val * 100.0

            # Higher adoption -> more data exposure -> more surveillance capitalism
            digital_stress = float(np.clip(internet_pct / 100.0, 0, 1)) * 0.5 + 0.1

            trend = None
            if len(vals) >= 3:
                t = np.arange(len(vals), dtype=float)
                slope, _, r_val, p_val, _ = stats.linregress(t, vals)
                trend = {
                    "slope": round(float(slope), 4),
                    "direction": "rising" if slope > 0 else "falling",
                    "r_squared": round(float(r_val ** 2), 4),
                }

            digital_analysis = {
                "latest_internet_penetration_pct": round(internet_pct, 2),
                "data_exposure_stress": round(digital_stress, 4),
                "n_obs": len(vals),
                "date_range": [str(dates[0]), str(dates[-1])],
                "note": "High internet penetration -> larger digital data economy footprint",
                "reference": "Acquisti et al. 2016: privacy paradox, revealed vs stated WTP",
            }
            if trend:
                digital_analysis["trend"] = trend

        # --- 2. Data market valuation (intangible assets as proxy) ---
        data_market = None
        data_market_stress = 0.4
        if intangible_rows:
            iv = np.array([float(r["value"]) for r in intangible_rows])
            int_dates = [r["date"] for r in intangible_rows]
            latest_int = float(iv[-1])

            # R&D/GDP typically 1-4%. ICT investment / intangibles growing.
            if latest_int > 1:
                int_pct = latest_int
            else:
                int_pct = latest_int * 100.0

            # Higher intangibles -> larger data economy
            data_market_stress = float(np.clip(int_pct / 5.0, 0, 1))

            data_market = {
                "latest_intangible_investment_pct": round(int_pct, 2),
                "data_market_stress": round(data_market_stress, 4),
                "n_obs": len(iv),
                "date_range": [str(int_dates[0]), str(int_dates[-1])],
                "reference": "Jones & Tonetti 2020: non-rivalry of data, optimal ownership",
            }

        # --- 3. Cybersecurity / GDPR compliance proxy ---
        compliance_analysis = None
        compliance_stress = 0.5
        if cyber_rows:
            cv = np.array([float(r["value"]) for r in cyber_rows])
            cyber_dates = [r["date"] for r in cyber_rows]
            latest_cy = float(cv[-1])

            # Higher cybersecurity spending = more privacy protection investment
            # But also signals greater underlying threat
            # Normalize and treat as partial offset
            if len(cv) >= 3:
                t = np.arange(len(cv), dtype=float)
                slope, _, r_val, _, _ = stats.linregress(t, cv)
                growing = float(slope) > 0
            else:
                growing = True

            compliance_stress = 0.4 if growing else 0.6  # Growing investment = lower stress

            compliance_analysis = {
                "latest_cybersecurity_value": round(latest_cy, 3),
                "investment_growing": growing,
                "compliance_stress": round(compliance_stress, 4),
                "n_obs": len(cv),
                "date_range": [str(cyber_dates[0]), str(cyber_dates[-1])],
                "reference": "Goldberg et al. 2019: GDPR reduced EU website traffic 7-10%",
            }

        # --- 4. Digital concentration / surveillance capitalism ---
        surveillance_analysis = None
        surveillance_stress = 0.5
        if concentration_rows:
            sv = np.array([float(r["value"]) for r in concentration_rows])
            surv_dates = [r["date"] for r in concentration_rows]
            latest_sv = float(sv[-1])

            if latest_sv > 100:
                surveillance_stress = float(np.clip(latest_sv / 10000.0, 0, 1))
            elif latest_sv > 1:
                surveillance_stress = float(np.clip(latest_sv / 100.0, 0, 1))
            else:
                surveillance_stress = float(np.clip(latest_sv, 0, 1))

            surveillance_analysis = {
                "latest_concentration": round(latest_sv, 3),
                "surveillance_stress": round(surveillance_stress, 4),
                "n_obs": len(sv),
                "date_range": [str(surv_dates[0]), str(surv_dates[-1])],
                "reference": "Acemoglu et al. 2022: AI/surveillance negative externalities",
            }

        # --- Score ---
        # Weights: digital exposure 25, data market 25, compliance 25, surveillance 25
        score = float(np.clip(
            digital_stress * 25.0
            + data_market_stress * 25.0
            + compliance_stress * 25.0
            + surveillance_stress * 25.0,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "digital_data_exposure": round(digital_stress * 25.0, 2),
                "data_market_size": round(data_market_stress * 25.0, 2),
                "compliance_burden": round(compliance_stress * 25.0, 2),
                "surveillance_concentration": round(surveillance_stress * 25.0, 2),
            },
        }

        if digital_analysis:
            result["digital_economy"] = digital_analysis
        if data_market:
            result["data_market_valuation"] = data_market
        if compliance_analysis:
            result["gdpr_compliance_proxy"] = compliance_analysis
        if surveillance_analysis:
            result["surveillance_capitalism"] = surveillance_analysis

        return result
