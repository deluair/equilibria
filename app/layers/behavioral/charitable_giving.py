"""Charitable Giving module.

Four dimensions of the economics of philanthropy and altruism:

1. **Warm glow utility** (Andreoni 1989, 1990):
   Donors gain utility from the act of giving itself (impure altruism),
   not just the public good provision. This explains why government provision
   does not fully crowd out private giving. Warm glow identified by the
   partial crowd-out empirical finding. Estimated from giving-to-income ratio
   and structural warm-glow parameter.

2. **Tax deduction elasticity** (Peloza & Steel 2005):
   Meta-analysis of 69 studies: price elasticity of charitable giving is
   approximately -1.1 (slightly elastic). After tax deduction:
   giving_price = 1 - marginal_tax_rate. A fall in price raises giving.
   Estimated via log-log regression of donations on (1 - MTR).

3. **Crowding out of government aid** (Andreoni & Payne 2011):
   Government grants to NGOs reduce private fundraising by $0.56 per dollar
   of grant (fundraising crowd-out) and total private donations by $0.26
   (direct crowd-out). Partial crowd-out consistent with warm glow.
   Estimated from correlation between public social expenditure and
   private giving.

4. **Effective altruism cost-effectiveness** (GiveWell, Singer 2009):
   GiveWell estimates cost per life saved for top charities: $3,000-$5,000
   (AMF malaria nets). Measured against GDP-based giving capacity and
   relative philanthropic efficiency.

Score: low giving-to-income ratio + inelastic tax response + full government
crowd-out + low cost-effectiveness -> high stress (inefficient philanthropy).

References:
    Andreoni, J. (1989). "Giving with Impure Altruism." JPE 97(6).
    Andreoni, J. (1990). "Impure Altruism and Donations to Public Goods."
        Economic Journal 100(401).
    Peloza, J. & Steel, P. (2005). "The Price Elasticities of Charitable
        Contributions." Journal of Public Policy & Marketing 24(1).
    Andreoni, J. & Payne, A. (2011). "Is Crowding Out Due Entirely to
        Fundraising?" Journal of Public Economics 95(5-6).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class CharitableGiving(LayerBase):
    layer_id = "l13"
    name = "Charitable Giving"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate warm glow, tax elasticity, and crowding-out of charitable giving.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default USA)
        """
        country = kwargs.get("country_iso3", "USA")

        # Charitable giving / donation data
        giving_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%charitable%giving%' OR ds.name LIKE '%philanthropy%'
                   OR ds.name LIKE '%donation%' OR ds.name LIKE '%nonprofit%revenue%'
                   OR ds.name LIKE '%civil%society%spending%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # GDP per capita for giving-to-income ratio
        gdp_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'fred', 'imf')
              AND ds.series_id IN ('NY.GDP.PCAP.KD', 'NY.GDP.PCAP.CD', 'GDPPC')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Social protection / government expenditure data (for crowd-out)
        gov_social_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'imf', 'oecd')
              AND (ds.name LIKE '%social%protection%expenditure%' OR ds.name LIKE '%social%spending%gdp%'
                   OR ds.name LIKE '%government%social%expenditure%' OR ds.name LIKE '%welfare%spending%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # Top marginal income tax rate (for price elasticity)
        tax_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND (ds.name LIKE '%top%marginal%tax%rate%' OR ds.name LIKE '%highest%income%tax%'
                   OR ds.name LIKE '%marginal%income%tax%' OR ds.name LIKE '%personal%income%tax%top%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not giving_rows and not gov_social_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no giving/philanthropy data"}

        # --- 1. Warm glow: giving-to-income ratio ---
        giving_analysis = None
        giving_stress = 0.5
        if giving_rows:
            gv_map: dict[str, list] = {}
            for r in giving_rows:
                gv_map.setdefault(r["series_id"], []).append((r["date"], float(r["value"])))

            primary_sid = max(gv_map, key=lambda s: len(gv_map[s]))
            primary = sorted(gv_map[primary_sid], key=lambda x: x[0])
            dates = [d for d, _ in primary]
            vals = np.array([v for _, v in primary])
            latest_val = float(vals[-1])

            # Normalize by GDP per capita if available
            if gdp_rows:
                gdp_map = {str(r["date"])[:4]: float(r["value"]) for r in gdp_rows}
                gdp_years = sorted(set([str(d)[:4] for d in dates]) & set(gdp_map.keys()))
                if gdp_years:
                    giving_vals_gdp = np.array([gv_map[primary_sid][i][1] for i, d in enumerate(dates)
                                                if str(d)[:4] in gdp_years[:len(gv_map[primary_sid])]])
                    gdp_arr = np.array([gdp_map[y] for y in gdp_years[:len(giving_vals_gdp)]])
                    if len(giving_vals_gdp) == len(gdp_arr) and np.mean(gdp_arr) > 0:
                        giving_ratio = float(np.mean(giving_vals_gdp) / np.mean(gdp_arr))
                        # US giving ~2% of GDP is high; <0.5% is low
                        giving_stress = float(np.clip(1.0 - giving_ratio / 0.02, 0, 1))
                    else:
                        giving_stress = 0.5
                else:
                    giving_stress = 0.5
            else:
                # Use trend as proxy: declining giving = more stress
                if len(vals) >= 3:
                    t = np.arange(len(vals), dtype=float)
                    slope, _, _, _, _ = stats.linregress(t, vals)
                    giving_stress = 0.6 if slope < 0 else 0.3
                else:
                    giving_stress = 0.5

            giving_analysis = {
                "latest_value": round(latest_val, 4),
                "mean_value": round(float(np.mean(vals)), 4),
                "giving_stress": round(giving_stress, 4),
                "n_obs": len(vals),
                "date_range": [str(dates[0]), str(dates[-1])],
                "reference": "Andreoni 1989, 1990: warm glow utility from giving",
            }

        # --- 2. Tax elasticity of giving ---
        tax_elasticity = None
        elasticity_stress = 0.4
        mtr = None
        if tax_rows:
            tv = np.array([float(r["value"]) for r in tax_rows])
            latest_mtr = float(tv[-1])
            # Store as 0-1
            mtr = latest_mtr / 100.0 if latest_mtr > 1 else latest_mtr

        if giving_rows and tax_rows and len(giving_rows) >= 5 and len(tax_rows) >= 5:
            gv_map2 = {str(r["date"])[:4]: float(r["value"]) for r in giving_rows}
            tx_map = {str(r["date"])[:4]: float(r["value"]) for r in tax_rows}
            common = sorted(set(gv_map2.keys()) & set(tx_map.keys()))

            if len(common) >= 5:
                g_arr = np.log(np.array([gv_map2[y] for y in common]) + 1e-10)
                # Price of giving = 1 - MTR (higher MTR = lower price)
                mtr_arr = np.array([tx_map[y] for y in common])
                if np.max(mtr_arr) > 1:
                    mtr_arr = mtr_arr / 100.0
                price_arr = np.log(1.0 - mtr_arr + 1e-10)

                slope, _, r_val, p_val, _ = stats.linregress(price_arr, g_arr)
                estimated_elasticity = float(slope)

                # Peloza-Steel meta-analysis: elasticity ~ -1.1
                # Stress if elasticity is inelastic (closer to 0)
                elasticity_stress = float(np.clip(
                    1.0 - abs(estimated_elasticity) / 1.1, 0, 1
                ))

                tax_elasticity = {
                    "estimated_price_elasticity": round(estimated_elasticity, 4),
                    "r_squared": round(float(r_val ** 2), 4),
                    "p_value": round(float(p_val), 4),
                    "current_mtr": round(mtr, 4) if mtr is not None else None,
                    "giving_price": round(1.0 - mtr, 4) if mtr is not None else None,
                    "peloza_steel_benchmark": -1.1,
                    "tax_responsive": estimated_elasticity < -0.5,
                    "n_obs": len(common),
                    "reference": "Peloza & Steel 2005 meta-analysis: mean elasticity -1.1",
                }

        # --- 3. Government crowd-out ---
        crowdout_analysis = None
        crowdout_stress = 0.4
        if gov_social_rows:
            sv = np.array([float(r["value"]) for r in gov_social_rows])
            soc_dates = [r["date"] for r in gov_social_rows]
            latest_soc = float(sv[-1])

            # Government social spending % GDP: OECD average ~20%
            if latest_soc > 1:
                soc_pct = latest_soc
            else:
                soc_pct = latest_soc * 100.0

            # Higher government spending -> more potential crowd-out
            crowdout_stress = float(np.clip(soc_pct / 30.0, 0, 1))

            # Test: correlation between government spending and private giving
            partial_crowdout = None
            if giving_rows and len(giving_rows) >= 5:
                gv_map3 = {str(r["date"])[:4]: float(r["value"]) for r in giving_rows}
                soc_map = {str(r["date"])[:4]: float(r["value"]) for r in gov_social_rows}
                common = sorted(set(gv_map3.keys()) & set(soc_map.keys()))
                if len(common) >= 5:
                    g_arr = np.array([gv_map3[y] for y in common])
                    s_arr = np.array([soc_map[y] for y in common])
                    slope, _, r_val, p_val, _ = stats.linregress(s_arr, g_arr)
                    partial_crowdout = {
                        "crowdout_coefficient": round(float(slope), 6),
                        "r_squared": round(float(r_val ** 2), 4),
                        "p_value": round(float(p_val), 4),
                        "partial_crowdout": float(slope) < 0,
                        "andreoni_payne_benchmark": -0.26,
                        "n_obs": len(common),
                    }

            crowdout_analysis = {
                "latest_gov_social_spending_pct": round(soc_pct, 2),
                "crowdout_stress": round(crowdout_stress, 4),
                "n_obs": len(sv),
                "date_range": [str(soc_dates[0]), str(soc_dates[-1])],
                "reference": "Andreoni & Payne 2011: $0.26 direct crowd-out per $1 government grant",
            }
            if partial_crowdout:
                crowdout_analysis["regression"] = partial_crowdout

        # --- Score ---
        # Weights: giving level 35, tax elasticity 30, crowd-out 35
        score = float(np.clip(
            giving_stress * 35.0
            + elasticity_stress * 30.0
            + crowdout_stress * 35.0,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "score_components": {
                "low_giving_ratio": round(giving_stress * 35.0, 2),
                "inelastic_tax_response": round(elasticity_stress * 30.0, 2),
                "government_crowdout": round(crowdout_stress * 35.0, 2),
            },
        }

        if giving_analysis:
            result["charitable_giving"] = giving_analysis
        if tax_elasticity:
            result["tax_elasticity"] = tax_elasticity
        if crowdout_analysis:
            result["government_crowdout"] = crowdout_analysis

        return result
