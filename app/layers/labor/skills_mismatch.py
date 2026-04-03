"""Skills mismatch: overeducation, obsolescence, and training returns.

Four analytical components:

1. Overeducation rates (realized-required method): share of workers whose
   attained education exceeds the modal/required level for their occupation.
   Duncan-Hoffman (1981) decomposition separates required, over, and under
   education contributions to wages.
   Overeducation penalty: ~0.03-0.08 log points per year of surplus schooling
   (vs 0.06-0.10 for required education). McGuinness (2006) meta-analysis.

2. Skill obsolescence index: rate at which existing skills lose market value.
   Proxied by: (a) share of occupations with declining task-complexity
   adjusted wage; (b) excess displacement rate in skill-intensive sectors;
   (c) divergence between educational degrees awarded and demand growth
   by field (STEM vs. non-STEM).

3. Training investment returns: employer-sponsored and government training
   expenditure as % GDP, combined with estimated return per trainee.
   Heckman-Lochner-Taber (1998): training returns highest for younger cohorts.
   Average social return ~9-15% IRR for well-targeted programs.

4. STEM premium dynamics: wage premium for STEM vs non-STEM degrees over
   time. Widening premium signals growing mismatch in non-STEM fields.
   Narrowing can indicate either STEM supply caught up or tech commoditized.

References:
    Duncan, G. & Hoffman, S. (1981). The incidence and wage effects of
        overeducation. Economics of Education Review 1(1): 75-86.
    McGuinness, S. (2006). Overeducation in the labour market.
        Journal of Economic Surveys 20(3): 387-418.
    Heckman, J., Lochner, L. & Taber, C. (1998). Explaining rising wage
        inequality. Review of Economic Dynamics 1(1): 1-58.
    Acemoglu, D. & Restrepo, P. (2018). The race between man and machine.
        AER 108(6): 1488-1542.

Score: high overeducation + rapid obsolescence + low training + widening
STEM gap -> STRESS. Low mismatch, active retraining -> STABLE.
"""

import json

import numpy as np

from app.layers.base import LayerBase


class SkillsMismatch(LayerBase):
    layer_id = "l3"
    name = "Skills Mismatch"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'skills_mismatch'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient skills mismatch data"}

        overeducation_rates = []
        undereducation_rates = []
        obsolescence_indices = []
        training_expenditures = []
        training_returns = []
        stem_premiums = []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            indicator = meta.get("indicator", row.get("description", ""))
            val = row["value"]
            if val is None:
                continue

            if "overeducation_rate" in indicator:
                overeducation_rates.append(float(val))
            elif "undereducation_rate" in indicator:
                undereducation_rates.append(float(val))
            elif "obsolescence_index" in indicator or "skill_obsolescence" in indicator:
                obsolescence_indices.append(float(val))
            elif "training_expenditure" in indicator or "training_gdp" in indicator:
                training_expenditures.append(float(val))
            elif "training_return" in indicator:
                training_returns.append(float(val))
            elif "stem_premium" in indicator:
                stem_premiums.append(float(val))

        if not overeducation_rates and not obsolescence_indices:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no mismatch indicators available"}

        overeducation = float(np.mean(overeducation_rates)) if overeducation_rates else None
        undereducation = float(np.mean(undereducation_rates)) if undereducation_rates else None
        obsolescence = float(np.mean(obsolescence_indices)) if obsolescence_indices else None
        training_exp = float(np.mean(training_expenditures)) if training_expenditures else None
        training_ret = float(np.mean(training_returns)) if training_returns else None

        # STEM premium trend (widening = increasing mismatch in non-STEM)
        stem_premium_current = float(stem_premiums[0]) if stem_premiums else None
        stem_premium_trend = None
        if len(stem_premiums) >= 4:
            sp_arr = np.array(list(reversed(stem_premiums)))
            t_idx = np.arange(len(sp_arr), dtype=float)
            X_t = np.column_stack([np.ones(len(sp_arr)), t_idx])
            beta = np.linalg.lstsq(X_t, sp_arr, rcond=None)[0]
            stem_premium_trend = float(beta[1])

        # Duncan-Hoffman decomposition proxy:
        # Mismatch cost = overeducation_rate * wage_penalty_per_year
        # Literature: overeducation penalty ~60% of required education return
        # Using 0.05 log-point per surplus year * 2 surplus years on average
        dh_wage_loss = None
        if overeducation is not None:
            # ~2 surplus years at ~5% penalty each -> ~10% wage loss for overeducated
            dh_wage_loss = overeducation * 0.10

        # Score construction
        score = 20.0  # baseline: low mismatch assumed

        # Overeducation contribution (0-30% rate -> 0-35 pts)
        if overeducation is not None:
            score += min(35.0, overeducation / 30.0 * 35.0)

        # Obsolescence contribution (0-1 index -> 0-30 pts)
        if obsolescence is not None:
            score += min(30.0, obsolescence * 30.0)

        # Training deficit (low investment penalized up to 20 pts)
        if training_exp is not None:
            # OECD avg ~0.2% GDP on training; <0.1% is low
            if training_exp < 0.10:
                score += 20.0
            elif training_exp < 0.20:
                score += 10.0
            elif training_exp >= 0.30:
                score -= 5.0

        # STEM premium widening (up to 15 pts)
        if stem_premium_trend is not None and stem_premium_trend > 0.01:
            score += min(15.0, stem_premium_trend * 500.0)

        score = max(0.0, min(100.0, score))

        result = {
            "score": round(score, 2),
            "country": country,
        }

        if overeducation is not None:
            result["overeducation"] = {
                "rate_pct": round(overeducation, 2),
                "undereducation_rate_pct": round(undereducation, 2) if undereducation is not None else None,
                "dh_estimated_wage_loss_pct": round(dh_wage_loss * 100.0, 2) if dh_wage_loss is not None else None,
                "interpretation": (
                    "high overeducation, significant mismatch" if overeducation > 25
                    else "moderate overeducation" if overeducation > 15
                    else "low overeducation"
                ),
            }

        if obsolescence is not None:
            result["obsolescence_index"] = {
                "value": round(obsolescence, 4),
                "interpretation": (
                    "rapid skill obsolescence" if obsolescence > 0.5
                    else "moderate obsolescence" if obsolescence > 0.25
                    else "slow obsolescence"
                ),
            }

        if training_exp is not None:
            result["training"] = {
                "expenditure_pct_gdp": round(training_exp, 3),
                "estimated_return_pct": round(training_ret, 2) if training_ret is not None else None,
                "adequacy": (
                    "adequate investment" if training_exp >= 0.20
                    else "below-average investment" if training_exp >= 0.10
                    else "critically low investment"
                ),
            }

        if stem_premium_current is not None:
            result["stem_premium"] = {
                "current_pct": round(stem_premium_current, 2),
                "trend_per_period": round(stem_premium_trend, 4) if stem_premium_trend is not None else None,
                "widening": stem_premium_trend is not None and stem_premium_trend > 0.005,
            }

        return result
