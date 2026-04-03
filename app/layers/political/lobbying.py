"""Grossman-Helpman protection for sale and lobbying economics.

Grossman & Helpman (1994) "Protection for Sale": tariff protection is a
function of import penetration, demand elasticity, and whether the industry
is politically organized (has a lobby).

    t_i / (1 + t_i) = -(1/a) * (z_i / e_i) + (1/a) * (I_i / (a+aL)) * (z_i / e_i)

where t_i is ad valorem tariff, z_i is inverse import penetration ratio,
e_i is import demand elasticity, I_i is an indicator for organized industry,
a is the government's weight on welfare vs contributions, aL is lobby share.

A low "a" means the government is highly susceptible to lobbying (corruption).
Empirically, a ~ 50-100 for the US (Goldberg-Maggi 1999), meaning welfare
is weighted 50-100x more than contributions.

Campaign contributions: Stratmann (2005) finds $1 in contributions yields
$10-50 in industry rents. The revolving door: Blanes i Vidal et al. (2012)
show lobbyists with government connections earn 24% premium.

Score: low welfare weight (a), high tariff distortion, high revolving door
intensity -> high stress.

References:
    Grossman, G. & Helpman, E. (1994). "Protection for Sale." AER 84(4).
    Goldberg, P. & Maggi, G. (1999). "Protection for Sale: An Empirical
        Investigation." AER 89(5).
    Stratmann, T. (2005). "Some Talk: Money in Politics." Public Choice 124.
    Blanes i Vidal, J., Draca, M. & Fons-Rosen, C. (2012). "Revolving Door
        Lobbyists." AER 102(7).
"""

from __future__ import annotations

import numpy as np
from scipy import optimize

from app.layers.base import LayerBase


class LobbyingEconomics(LayerBase):
    layer_id = "l12"
    name = "Lobbying Economics"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate Grossman-Helpman protection for sale parameters.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default USA)
            year : int - reference year
        """
        country = kwargs.get("country_iso3", "USA")
        year = kwargs.get("year", 2022)

        # Fetch industry-level tariff, import penetration, elasticity data
        rows = await db.fetch_all(
            """
            SELECT dp.value, ds.name, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND CAST(SUBSTR(dp.date, 1, 4) AS INTEGER) = ?
              AND ds.source IN ('comtrade', 'wto', 'wdi', 'usitc')
              AND (ds.name LIKE '%tariff%' OR ds.name LIKE '%import%penetration%'
                   OR ds.name LIKE '%import%elasticity%' OR ds.name LIKE '%lobby%'
                   OR ds.name LIKE '%campaign%contribution%' OR ds.name LIKE '%organized%')
            ORDER BY ds.name
            """,
            (country, year),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no lobbying/tariff data"}

        # Parse industry-level data
        industries: dict[str, dict] = {}
        aggregate_contributions = 0.0
        aggregate_lobby_spending = 0.0

        for r in rows:
            name = r["name"].lower()
            val = float(r["value"]) if r["value"] is not None else 0.0
            # Extract industry from metadata or name
            meta = r["metadata"] or ""
            industry = meta if meta else "aggregate"

            if industry not in industries:
                industries[industry] = {}

            if "tariff" in name:
                industries[industry]["tariff"] = val
            elif "import" in name and "penetration" in name:
                industries[industry]["import_penetration"] = val
            elif "elasticity" in name:
                industries[industry]["elasticity"] = val
            elif "organized" in name or "lobby" in name and "spending" not in name:
                industries[industry]["organized"] = int(val > 0)
            elif "campaign" in name or "contribution" in name:
                aggregate_contributions += val
            elif "lobby" in name and "spending" in name:
                aggregate_lobby_spending += val

        # --- Grossman-Helpman estimation ---
        # Collect industries with sufficient data
        tariffs = []
        inv_penetrations = []
        elasticities = []
        organized_flags = []

        for ind, data in industries.items():
            if "tariff" in data and "import_penetration" in data and "elasticity" in data:
                t = data["tariff"] / 100.0  # Convert pct to decimal
                ip = data["import_penetration"]
                e = data["elasticity"]
                org = data.get("organized", 0)
                if ip > 0 and abs(e) > 0:
                    tariffs.append(t)
                    inv_penetrations.append(1.0 / ip)  # z_i = 1/import_penetration
                    elasticities.append(abs(e))
                    organized_flags.append(org)

        gh_result = None
        welfare_weight = None

        if len(tariffs) >= 5:
            tariffs_arr = np.array(tariffs)
            z = np.array(inv_penetrations)
            e = np.array(elasticities)
            org = np.array(organized_flags)

            # LHS: t_i / (1 + t_i)
            lhs = tariffs_arr / (1.0 + tariffs_arr)
            # RHS regressors: z/e and I*z/e
            ze = z / e
            org_ze = org * ze

            if org.sum() > 0 and org.sum() < len(org):
                X = np.column_stack([np.ones(len(lhs)), ze, org_ze])
                beta = np.linalg.lstsq(X, lhs, rcond=None)[0]

                # beta[1] = -1/a, beta[2] captures organized industry premium
                if abs(beta[1]) > 1e-10:
                    a_hat = -1.0 / beta[1]
                    welfare_weight = abs(a_hat)

                resid = lhs - X @ beta
                r_squared = 1.0 - np.sum(resid ** 2) / np.sum((lhs - np.mean(lhs)) ** 2) if np.var(lhs) > 0 else 0.0

                # Organized industry tariff premium
                org_premium = float(np.mean(tariffs_arr[org == 1]) - np.mean(tariffs_arr[org == 0])) if org.sum() > 0 and org.sum() < len(org) else 0.0

                gh_result = {
                    "welfare_weight_a": round(welfare_weight, 2) if welfare_weight else None,
                    "organized_premium_pct": round(org_premium * 100, 4),
                    "r_squared": round(float(r_squared), 4),
                    "n_industries": len(tariffs),
                    "n_organized": int(org.sum()),
                    "coefficients": {
                        "constant": round(float(beta[0]), 6),
                        "z_over_e": round(float(beta[1]), 6),
                        "organized_z_over_e": round(float(beta[2]), 6),
                    },
                }

        # --- Campaign contribution returns ---
        contribution_return = None
        if aggregate_contributions > 0 and aggregate_lobby_spending > 0:
            # Stratmann: $1 in contributions -> $10-50 in rents (stylized)
            # Ratio of spending to contributions indicates lobbying intensity
            intensity_ratio = aggregate_lobby_spending / aggregate_contributions
            contribution_return = {
                "total_contributions": round(aggregate_contributions, 0),
                "total_lobby_spending": round(aggregate_lobby_spending, 0),
                "spending_to_contribution_ratio": round(intensity_ratio, 2),
                "note": "Stratmann (2005): $1 contribution yields $10-50 in rents",
            }

        # --- Revolving door proxy ---
        revolving_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('opensecrets', 'vdem', 'wgi')
              AND (ds.name LIKE '%revolving%door%' OR ds.name LIKE '%regulatory%capture%'
                   OR ds.name LIKE '%corruption%perception%')
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        revolving_door = None
        if revolving_rows:
            vals = [float(r["value"]) for r in revolving_rows if r["value"] is not None]
            if vals:
                revolving_door = {
                    "latest_score": round(vals[0], 2),
                    "mean_score": round(float(np.mean(vals)), 2),
                    "note": "Higher values indicate more revolving-door activity",
                }

        # --- Score ---
        score_parts = []

        # Welfare weight: low a -> high susceptibility to lobbying
        if welfare_weight is not None:
            if welfare_weight < 10:
                score_parts.append(45.0)  # Very susceptible
            elif welfare_weight < 50:
                score_parts.append(30.0)
            elif welfare_weight < 100:
                score_parts.append(15.0)
            else:
                score_parts.append(5.0)  # Government strongly weights welfare
        else:
            score_parts.append(25.0)  # No data, neutral

        # Tariff distortion
        if tariffs:
            mean_tariff = np.mean(tariffs) * 100
            tariff_score = float(np.clip(mean_tariff * 2, 0, 30))
            score_parts.append(tariff_score)
        else:
            score_parts.append(10.0)

        # Lobbying intensity
        if contribution_return:
            intensity = contribution_return["spending_to_contribution_ratio"]
            lobby_score = float(np.clip(intensity * 5, 0, 25))
            score_parts.append(lobby_score)
        else:
            score_parts.append(10.0)

        score = float(np.clip(sum(score_parts), 0, 100))

        result = {
            "score": round(score, 2),
            "country": country,
            "year": year,
        }

        if gh_result:
            result["grossman_helpman"] = gh_result
        if contribution_return:
            result["campaign_contributions"] = contribution_return
        if revolving_door:
            result["revolving_door"] = revolving_door

        return result
