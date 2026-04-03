"""Kaplan-Meier survival analysis of export relationships.

Methodology:
    Analyze the duration of bilateral export relationships using survival
    analysis techniques. Following Besedes and Prusa (2006), we:

    1. Define export spells: consecutive years of positive bilateral exports
       for a given product/partner pair.
    2. Estimate Kaplan-Meier survival function: S(t) = prod_{t_i<=t} (1 - d_i/n_i)
       where d_i = number of failures at time t_i, n_i = number at risk.
    3. Compute hazard rates: h(t) = d_i / n_i at each failure time.
    4. Identify factors associated with longer/shorter export duration.

    Score (0-100): Higher score means shorter average export spell duration
    (more fragile trade relationships).

References:
    Besedes, T. and Prusa, T.J. (2006). "Product differentiation and
        duration of US import trade." Journal of International Economics,
        70(2), 339-358.
    Nitsch, V. (2009). "Die another day: Duration in German import trade."
        Review of World Economics, 145(1), 133-154.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ExportSurvival(LayerBase):
    layer_id = "l1"
    name = "Export Survival Analysis"

    async def compute(self, db, **kwargs) -> dict:
        """Compute Kaplan-Meier survival estimates for export relationships.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 country code
            year_start : int - start of observation window
            year_end : int - end of observation window
        """
        reporter = kwargs.get("reporter", "USA")
        year_start = kwargs.get("year_start", 2000)
        year_end = kwargs.get("year_end", 2022)

        # Fetch export spell data
        rows = await db.execute(
            """
            SELECT partner_iso3, product_code, year, trade_value
            FROM bilateral_trade
            WHERE reporter_iso3 = ?
              AND year BETWEEN ? AND ?
              AND trade_value > 0
            ORDER BY partner_iso3, product_code, year
            """,
            (reporter, year_start, year_end),
        )
        records = await rows.fetchall()

        if not records:
            return {"score": 50.0, "median_survival": None,
                    "note": "No export data available"}

        # Build spells: consecutive years of positive trade
        spells: dict[tuple[str, str], list[list[int]]] = {}
        for r in records:
            key = (r["partner_iso3"], r["product_code"])
            yr = int(r["year"])
            if key not in spells:
                spells[key] = [[yr]]
            else:
                last_spell = spells[key][-1]
                if yr == last_spell[-1] + 1:
                    last_spell.append(yr)
                else:
                    spells[key].append([yr])

        # Compute spell durations and censoring
        durations = []
        censored = []  # 1 if spell is right-censored (still active at year_end)
        for key, spell_list in spells.items():
            for spell in spell_list:
                dur = len(spell)
                is_censored = spell[-1] >= year_end
                durations.append(dur)
                censored.append(1 if is_censored else 0)

        durations_arr = np.array(durations)
        censored_arr = np.array(censored)

        if len(durations_arr) == 0:
            return {"score": 50.0, "median_survival": None,
                    "note": "No export spells found"}

        # Kaplan-Meier estimator
        max_t = int(durations_arr.max())
        unique_times = np.arange(1, max_t + 1)

        n_at_risk = np.zeros(max_t)
        n_events = np.zeros(max_t)

        for t_idx, t in enumerate(unique_times):
            # At risk: spells with duration >= t
            n_at_risk[t_idx] = np.sum(durations_arr >= t)
            # Events: spells ending at exactly t and not censored
            n_events[t_idx] = np.sum(
                (durations_arr == t) & (censored_arr == 0)
            )

        # Survival function: S(t) = prod (1 - d_i/n_i)
        hazard = np.zeros(max_t)
        survival = np.ones(max_t)
        for t_idx in range(max_t):
            if n_at_risk[t_idx] > 0:
                hazard[t_idx] = n_events[t_idx] / n_at_risk[t_idx]
            survival[t_idx] = (
                (1 - hazard[t_idx]) if t_idx == 0
                else survival[t_idx - 1] * (1 - hazard[t_idx])
            )

        # Median survival time: first t where S(t) <= 0.5
        below_half = np.where(survival <= 0.5)[0]
        median_survival = int(unique_times[below_half[0]]) if len(below_half) > 0 else max_t

        # Mean duration
        mean_duration = float(np.mean(durations_arr))

        # 1-year and 5-year survival rates
        surv_1yr = float(survival[0]) if max_t >= 1 else 1.0
        surv_5yr = float(survival[4]) if max_t >= 5 else float(survival[-1])

        # Hazard by duration
        hazard_table = [
            {"year": int(unique_times[i]), "hazard": float(hazard[i]),
             "survival": float(survival[i]), "at_risk": int(n_at_risk[i])}
            for i in range(min(max_t, 20))
        ]

        # Score: short spells = high vulnerability
        # Benchmark: median survival of 10 years = stable (score 0)
        # median survival of 1 year = fragile (score 100)
        score = float(np.clip((1 - (median_survival - 1) / 9) * 100, 0, 100))

        n_total_spells = len(durations_arr)
        n_censored = int(censored_arr.sum())
        n_unique_partners = len(set(k[0] for k in spells.keys()))
        n_unique_products = len(set(k[1] for k in spells.keys()))

        return {
            "score": score,
            "median_survival": median_survival,
            "mean_duration": mean_duration,
            "survival_1yr": surv_1yr,
            "survival_5yr": surv_5yr,
            "n_spells": n_total_spells,
            "n_censored": n_censored,
            "n_unique_partners": n_unique_partners,
            "n_unique_products": n_unique_products,
            "hazard_table": hazard_table,
            "max_observed_duration": int(max_t),
            "reporter": reporter,
            "year_start": year_start,
            "year_end": year_end,
        }
