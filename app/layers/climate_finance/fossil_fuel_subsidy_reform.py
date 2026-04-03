"""Fossil fuel subsidy reform: subsidies as % GDP, reform progress, political economy.

Methodology
-----------
**Subsidy measurement** (IMF post-tax methodology, Coady et al. 2019):
    post_tax_subsidy = pre_tax_explicit + CO2_externality + local_pollution
                       + congestion + accident_costs + forgone_tax

    Global post-tax subsidies: ~$7 trillion/yr (7.1% global GDP) in 2022
    per IMF WP/23/169. Pre-tax (explicit) subsidies ~$1 trillion/yr.

**Reform progress index**:
    Tracks whether subsidies are declining as a share of GDP over time.
    Positive trend (declining) = reform underway.
    Negative trend (rising) = deteriorating.

    reform_progress = -(slope of subsidy_pct_gdp over last 5 years)
    Normalized to [-1, 1]: +1 = rapid reform, -1 = rapid backsliding.

**Fiscal cost trajectory**:
    Whether subsidy fiscal cost crowds out productive public investment
    in health, education, infrastructure.
    crowding_out_proxy = subsidy_pct_gdp / public_investment_pct_gdp

Score: large subsidies with no reform raises score (crisis). Countries with
declining subsidies trending to zero score low.

Sources: IMF WP/23/169 (Agarwal et al.), IEA Fossil Fuel Subsidies Database,
OECD, World Bank
"""

import numpy as np

from app.layers.base import LayerBase

_SQL = """
    SELECT dp.date, dp.value
    FROM data_points dp
    JOIN data_series ds ON dp.series_id = ds.id
    WHERE ds.code = ?
    ORDER BY dp.date
"""


class FossilFuelSubsidyReform(LayerBase):
    layer_id = "lGF"
    name = "Fossil Fuel Subsidy Reform"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "WLD")

        codes = {
            "subsidy_pct_gdp": f"FOSSIL_SUBSIDY_PCT_GDP_{country}",
            "subsidy_usd": f"FOSSIL_SUBSIDY_USD_{country}",
            "public_investment_pct_gdp": f"PUBLIC_INVESTMENT_PCT_GDP_{country}",
        }

        data: dict[str, dict] = {}
        for key, code in codes.items():
            rows = await db.fetch_all(_SQL, (code,))
            if rows:
                data[key] = {r["date"]: float(r["value"]) for r in rows}

        if "subsidy_pct_gdp" not in data:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No fossil fuel subsidy as % GDP data",
            }

        sub_series = data["subsidy_pct_gdp"]
        dates = sorted(sub_series.keys())
        latest = dates[-1]
        current_pct = sub_series[latest]

        # Reform trend: slope over last 5 years
        reform_progress = None
        if len(dates) >= 5:
            recent = dates[-5:]
            values = np.array([sub_series[d] for d in recent], dtype=float)
            t = np.arange(len(recent), dtype=float)
            slope = float(np.polyfit(t, values, 1)[0])
            # Negative slope = declining subsidies = reform progress
            # Normalize: slope of -2 %GDP/yr = strong reform (+1), +2 = backsliding (-1)
            reform_progress = float(np.clip(-slope / 2.0, -1.0, 1.0))

        # Crowding out
        crowding_out = None
        if "public_investment_pct_gdp" in data:
            pi_vals = list(data["public_investment_pct_gdp"].values())
            pub_inv = float(pi_vals[-1]) if pi_vals else None
            if pub_inv and pub_inv > 0:
                crowding_out = current_pct / pub_inv

        subsidy_usd = None
        if "subsidy_usd" in data:
            usd_vals = list(data["subsidy_usd"].values())
            subsidy_usd = float(usd_vals[-1]) if usd_vals else None

        # Score: subsidy size (0-70) + reform direction (0-30)
        # 10% GDP = 70 pts (very large); 0% = 0 pts
        size_score = min(current_pct * 7, 70)

        # Reform direction: if declining, subtract up to 30; if rising, add up to 30
        reform_score = 0.0
        if reform_progress is not None:
            reform_score = -reform_progress * 30  # positive reform -> negative score adjustment

        score = min(max(size_score + reform_score, 0), 100)

        return {
            "score": round(score, 1),
            "metrics": {
                "country": country,
                "reference_date": latest,
                "fossil_subsidy_pct_gdp": round(current_pct, 2),
                "fossil_subsidy_usd_bn": round(subsidy_usd, 2) if subsidy_usd is not None else None,
                "reform_progress_index": round(reform_progress, 3) if reform_progress is not None else None,
                "reform_direction": (
                    "improving" if reform_progress and reform_progress > 0.1
                    else "deteriorating" if reform_progress and reform_progress < -0.1
                    else "stable"
                ),
                "crowding_out_ratio": round(crowding_out, 2) if crowding_out is not None else None,
            },
        }
