"""Bartik (shift-share) instrument construction and diagnostics.

The Bartik instrument (Bartik 1991) combines national industry growth rates
(shifts) with local industry employment shares (shares) to construct a
predicted local employment change that is driven by national trends rather
than local shocks.

Construction:
    B_r = sum_k (share_{r,k,0} * growth_{k,-r})

where share_{r,k,0} is region r's initial employment share in industry k,
and growth_{k,-r} is national (leave-one-out) growth in industry k.

This serves as an instrument for local labor demand shocks when estimating
effects of employment changes on wages, migration, etc.

Goldsmith-Pinkham, Sorkin & Swift (2020) diagnostics:
    - The Bartik instrument is valid iff the shares are exogenous
    - Rotemberg weights identify which industries drive identification
    - Pre-trend tests on high-weight industries

Borusyak, Hull & Jaravel (2022) alternative:
    - Reframe as exposure design: shocks (shifts) are exogenous, shares
      determine exposure. Inference is on the shocks.

References:
    Bartik, T. (1991). Who Benefits from State and Local Economic
        Development Policies? W.E. Upjohn Institute.
    Goldsmith-Pinkham, P., Sorkin, I. & Swift, H. (2020). Bartik
        Instruments: What, When, Why, and How. AER 110(8): 2586-2624.
    Borusyak, K., Hull, P. & Jaravel, X. (2022). Quasi-Experimental
        Shift-Share Research Designs. Review of Economic Studies 89(1).

Score: large Bartik shock dispersion -> STRESS (sectoral reallocation pressure).
Low dispersion -> STABLE.
"""

import numpy as np
from app.layers.base import LayerBase


class ShiftShareAnalysis(LayerBase):
    layer_id = "l3"
    name = "Shift-Share (Bartik) Analysis"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params = [country, "shift_share"]
        if year:
            params.append(str(year))

        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = ?
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient shift-share data"}

        import json

        regions = {}
        industries = set()

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            region = meta.get("region")
            industry = meta.get("industry")
            share_t0 = meta.get("initial_share")
            national_growth = meta.get("national_growth")
            local_growth = meta.get("local_growth")
            if region is None or industry is None:
                continue
            if share_t0 is None or national_growth is None:
                continue

            industries.add(industry)
            if region not in regions:
                regions[region] = {"shares": {}, "national_growths": {}, "local_growth": None}
            regions[region]["shares"][industry] = float(share_t0)
            regions[region]["national_growths"][industry] = float(national_growth)
            if local_growth is not None:
                regions[region]["local_growth"] = float(local_growth)

        if len(regions) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient regions"}

        industry_list = sorted(industries)
        n_regions = len(regions)
        n_industries = len(industry_list)

        # Construct Bartik instrument for each region
        bartik_instruments = {}
        shares_matrix = np.zeros((n_regions, n_industries))
        growth_vector = np.zeros(n_industries)
        local_growths = []
        region_names = sorted(regions.keys())

        for i, region in enumerate(region_names):
            reg_data = regions[region]
            bartik = 0.0
            for j, ind in enumerate(industry_list):
                s = reg_data["shares"].get(ind, 0.0)
                g = reg_data["national_growths"].get(ind, 0.0)
                shares_matrix[i, j] = s
                if i == 0:
                    growth_vector[j] = g
                bartik += s * g
            bartik_instruments[region] = bartik
            if reg_data["local_growth"] is not None:
                local_growths.append((bartik, reg_data["local_growth"]))

        bartik_values = np.array([bartik_instruments[r] for r in region_names])

        # Rotemberg weights: identify which industries drive identification
        # Weight_k = (growth_k * sum_r share_rk * ...) / sum_k(...)
        rotemberg_weights = {}
        total_weight = 0.0
        for j, ind in enumerate(industry_list):
            g_k = growth_vector[j]
            share_sum = float(np.sum(shares_matrix[:, j]))
            w_k = abs(g_k * share_sum)
            rotemberg_weights[ind] = w_k
            total_weight += w_k

        if total_weight > 0:
            rotemberg_weights = {k: round(v / total_weight, 4) for k, v in rotemberg_weights.items()}

        # Top-5 industries by Rotemberg weight
        top_industries = sorted(rotemberg_weights.items(), key=lambda x: -x[1])[:5]

        # First-stage F-stat if local growth data available
        first_stage_f = None
        if len(local_growths) >= 5:
            bl = np.array(local_growths)
            bartik_arr = bl[:, 0]
            local_arr = bl[:, 1]
            X_fs = np.column_stack([np.ones(len(bartik_arr)), bartik_arr])
            beta_fs = np.linalg.lstsq(X_fs, local_arr, rcond=None)[0]
            resid_fs = local_arr - X_fs @ beta_fs
            ss_res = np.sum(resid_fs ** 2)
            ss_tot = np.sum((local_arr - local_arr.mean()) ** 2)
            k_fs = 1
            n_fs = len(bartik_arr)
            if ss_res > 0:
                first_stage_f = float((ss_tot - ss_res) / k_fs) / (ss_res / (n_fs - 2))

        # Score: Bartik dispersion maps to reallocation pressure
        bartik_dispersion = float(np.std(bartik_values))
        mean_bartik = float(np.mean(bartik_values))

        if bartik_dispersion > 0.05:
            score = 50.0 + bartik_dispersion * 400.0
        elif bartik_dispersion > 0.02:
            score = 25.0 + (bartik_dispersion - 0.02) * 833.0
        else:
            score = bartik_dispersion * 1250.0
        score = max(0.0, min(100.0, score))

        return {
            "score": round(score, 2),
            "country": country,
            "n_regions": n_regions,
            "n_industries": n_industries,
            "bartik_instrument": {
                "mean": round(mean_bartik, 4),
                "std": round(bartik_dispersion, 4),
                "min": round(float(np.min(bartik_values)), 4),
                "max": round(float(np.max(bartik_values)), 4),
            },
            "rotemberg_weights_top5": [
                {"industry": ind, "weight": w} for ind, w in top_industries
            ],
            "first_stage_f": round(first_stage_f, 2) if first_stage_f is not None else None,
            "weak_instrument": first_stage_f is not None and first_stage_f < 10,
        }
