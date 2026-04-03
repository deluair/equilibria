"""Remote work adoption, productivity effects, and spatial wage implications.

Four analytical dimensions:

1. Remote work adoption rates: share of jobs performed remotely (full or
   hybrid), decomposed by sector and skill tier. Dingel & Neiman (2020)
   task-feasibility approach identifies which occupations can be done from
   home based on O*NET task content.

2. Productivity effects: quasi-experimental estimates of remote vs on-site
   productivity. Bloom et al. (2015) call-center RCT: +13% productivity
   gain. Later RCTs show heterogeneous effects (positive for focused tasks,
   negative for collaborative). Meta-regression estimate from WFH Research
   (Barrero-Bloom-Davis 2023): approximately +3-4% aggregate.

3. Wage geography premium erosion: pre-2020 urban wage premium (~20-30% for
   major metros) has compressed for remote-eligible workers. Identifies
   whether the local nominal wage premium has declined as workers decouple
   from location.

4. Urban-rural rebalancing: net migration flows from core metros to secondary
   cities and rural areas. Barrero-Bloom-Davis (2023) estimate ~5% of US
   workforce permanently relocated to lower-cost areas by 2023.

References:
    Dingel, J. & Neiman, B. (2020). How many jobs can be done at home?
        Journal of Public Economics 189: 104235.
    Bloom, N., Liang, J., Roberts, J. & Ying, Z.J. (2015). Does working
        from home work? QJE 130(1): 165-218.
    Barrero, J.M., Bloom, N. & Davis, S.J. (2023). The evolution of work
        from home. Journal of Economic Perspectives 37(4): 23-49.
    Autor, D. & Reynolds, E. (2020). The nature of work after the COVID
        crisis. Hamilton Project essay.

Score: widespread adoption with productivity gains and geographic rebalancing
-> STABLE (positive adjustment). Very low adoption or large productivity loss
-> moderate STRESS. Hybrid models score best.
"""

import json

import numpy as np

from app.layers.base import LayerBase


class RemoteWork(LayerBase):
    layer_id = "l3"
    name = "Remote Work Adoption & Effects"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'remote_work'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient remote work data"}

        remote_shares = []
        hybrid_shares = []
        productivity_effects = []
        urban_wage_premiums = []
        relocation_rates = []
        wfh_feasible_shares = []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            indicator = meta.get("indicator", row.get("description", ""))
            val = row["value"]
            if val is None:
                continue

            if "remote_share" in indicator or "fully_remote" in indicator:
                remote_shares.append(float(val))
            elif "hybrid_share" in indicator:
                hybrid_shares.append(float(val))
            elif "productivity_effect" in indicator:
                productivity_effects.append(float(val))
            elif "urban_wage_premium" in indicator:
                urban_wage_premiums.append(float(val))
            elif "relocation_rate" in indicator:
                relocation_rates.append(float(val))
            elif "wfh_feasible" in indicator or "dingel_neiman" in indicator:
                wfh_feasible_shares.append(float(val))

        if not remote_shares and not hybrid_shares:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no remote work adoption data"}

        remote_share = float(np.mean(remote_shares)) if remote_shares else 0.0
        hybrid_share = float(np.mean(hybrid_shares)) if hybrid_shares else 0.0
        total_flexible = remote_share + hybrid_share

        # Dingel-Neiman feasibility ceiling
        wfh_feasible = float(np.mean(wfh_feasible_shares)) if wfh_feasible_shares else None

        # Adoption gap: how much of feasible work is actually done remotely
        adoption_gap = None
        if wfh_feasible is not None and wfh_feasible > 0:
            adoption_gap = max(0.0, wfh_feasible - total_flexible)

        # Productivity effect (positive = above baseline)
        prod_effect = float(np.mean(productivity_effects)) if productivity_effects else None

        # Urban wage premium compression
        current_urban_premium = float(urban_wage_premiums[0]) if urban_wage_premiums else None
        premium_trend = None
        if len(urban_wage_premiums) >= 3:
            prem_arr = np.array(list(reversed(urban_wage_premiums)))
            t_idx = np.arange(len(prem_arr), dtype=float)
            X_t = np.column_stack([np.ones(len(prem_arr)), t_idx])
            beta = np.linalg.lstsq(X_t, prem_arr, rcond=None)[0]
            premium_trend = float(beta[1])

        relocation_rate = float(np.mean(relocation_rates)) if relocation_rates else None

        # Score: lower is better (stable/productive adaptation)
        # Penalize: low adoption gap relative to feasible (under-utilization),
        # negative productivity effects, no geographic rebalancing
        # Reward: hybrid adoption (research shows hybrid is optimal), productivity gains

        score = 50.0  # neutral baseline

        # Adoption: high flexible work with productivity gains is stable
        if total_flexible > 0.30:
            score -= 15.0
        elif total_flexible > 0.15:
            score -= 7.0
        elif total_flexible < 0.05:
            score += 15.0  # structural lag

        # Productivity effect
        if prod_effect is not None:
            if prod_effect > 0.05:
                score -= 10.0  # gains confirmed
            elif prod_effect > 0:
                score -= 3.0
            elif prod_effect < -0.05:
                score += 15.0  # significant loss
            elif prod_effect < 0:
                score += 5.0

        # Urban wage premium compression (declining premium = geographic equity)
        if premium_trend is not None and premium_trend < -0.005:
            score -= 5.0  # premium eroding (positive for equality)

        score = max(0.0, min(100.0, score))

        result = {
            "score": round(score, 2),
            "country": country,
            "adoption": {
                "fully_remote_share": round(remote_share, 4),
                "hybrid_share": round(hybrid_share, 4),
                "total_flexible_pct": round(total_flexible * 100.0, 2),
                "wfh_feasible_pct": round(wfh_feasible * 100.0, 2) if wfh_feasible is not None else None,
                "adoption_gap_pct": round(adoption_gap * 100.0, 2) if adoption_gap is not None else None,
            },
        }

        if prod_effect is not None:
            result["productivity"] = {
                "effect_pct": round(prod_effect * 100.0, 2),
                "interpretation": (
                    "confirmed productivity gains" if prod_effect > 0.03
                    else "modest gain" if prod_effect > 0
                    else "productivity loss detected"
                ),
            }

        if current_urban_premium is not None:
            result["wage_geography"] = {
                "current_urban_premium_pct": round(current_urban_premium, 2),
                "trend_per_period": round(premium_trend, 4) if premium_trend is not None else None,
                "compressing": premium_trend is not None and premium_trend < -0.002,
            }

        if relocation_rate is not None:
            result["urban_rural_rebalancing"] = {
                "relocation_rate_pct": round(relocation_rate, 2),
                "interpretation": (
                    "significant geographic rebalancing" if relocation_rate > 3.0
                    else "modest rebalancing" if relocation_rate > 1.0
                    else "minimal spatial adjustment"
                ),
            }

        return result
