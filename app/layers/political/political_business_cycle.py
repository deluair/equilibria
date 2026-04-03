"""Nordhaus opportunistic cycle and Hibbs partisan theory.

Nordhaus (1975) political business cycle: incumbents expand fiscal/monetary
policy before elections to boost short-run growth, accepting inflation after.
The prediction is counter-cyclical fiscal tightening post-election and
expansion pre-election.

Test:
    g_t = a0 + a1 * election_dummy_t + a2 * g_{t-1} + e_t

where election_dummy = 1 in the 4 quarters before an election. If a1 > 0
and significant, the opportunistic cycle is present.

Hibbs (1977) partisan theory: left-wing governments target lower unemployment
(accepting higher inflation), right-wing target lower inflation (accepting
higher unemployment).

Test:
    u_t = b0 + b1 * left_dummy_t + b2 * u_{t-1} + e_t

Central bank independence (CBI) should attenuate both cycles. Alesina &
Summers (1993) show high-CBI countries have lower inflation without higher
unemployment variance.

Score: strong election-cycle fiscal expansion + low CBI -> high stress.

References:
    Nordhaus, W. (1975). "The Political Business Cycle." RES 42(2).
    Hibbs, D. (1977). "Political Parties and Macroeconomic Policy." APSR 71(4).
    Alesina, A. & Summers, L. (1993). "Central Bank Independence and
        Macroeconomic Performance." JMCB 25(2).
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class PoliticalBusinessCycle(LayerBase):
    layer_id = "l12"
    name = "Political Business Cycle"

    async def compute(self, db, **kwargs) -> dict:
        """Detect Nordhaus/Hibbs political cycles.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 code (default BGD)
            election_years : list[int] - known election years (optional)
            cbi_score : float - central bank independence 0-1 (optional)
        """
        country = kwargs.get("country_iso3", "BGD")
        election_years = kwargs.get("election_years")
        cbi_score = kwargs.get("cbi_score")

        # Fetch GDP growth series
        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'imf', 'fred')
              AND (ds.name LIKE '%gdp%growth%' OR ds.name LIKE '%real gdp%growth%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient GDP growth data"}

        dates = [r["date"] for r in rows]
        growth = np.array([float(r["value"]) for r in rows])
        years = np.array([int(d[:4]) if isinstance(d, str) else int(d) for d in dates])

        # Fetch fiscal balance for fiscal expansion test
        fiscal_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'imf', 'fred')
              AND (ds.name LIKE '%fiscal%balance%' OR ds.name LIKE '%budget%balance%'
                   OR ds.name LIKE '%government%expenditure%gdp%')
            ORDER BY dp.date
            """,
            (country,),
        )

        # If no election years provided, try to infer from data
        if election_years is None:
            elec_rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.source IN ('vdem', 'dpi', 'wdi')
                  AND (ds.name LIKE '%election%' OR ds.name LIKE '%political%cycle%')
                ORDER BY dp.date
                """,
                (country,),
            )
            if elec_rows:
                election_years = sorted({int(str(r["date"])[:4]) for r in elec_rows if r["value"]})

        # --- Nordhaus opportunistic cycle test ---
        nordhaus_result = None
        if election_years and len(election_years) >= 2:
            election_set = set(election_years)
            # Election dummy: 1 in election year and year before
            pre_election = np.array(
                [1.0 if (y in election_set or y + 1 in election_set) else 0.0 for y in years]
            )

            if len(growth) >= 5 and pre_election.sum() > 0 and pre_election.sum() < len(pre_election):
                # OLS: g_t = a0 + a1*election + a2*g_{t-1}
                g_lag = np.roll(growth, 1)
                # Drop first observation (no lag)
                y = growth[1:]
                X = np.column_stack([np.ones(len(y)), pre_election[1:], g_lag[1:]])

                beta = np.linalg.lstsq(X, y, rcond=None)[0]
                resid = y - X @ beta
                n, k = X.shape
                se = np.sqrt(np.sum(resid ** 2) / max(n - k, 1) * np.diag(np.linalg.inv(X.T @ X + np.eye(k) * 1e-10)))

                t_stat_election = beta[1] / se[1] if se[1] > 0 else 0.0
                p_val_election = float(2 * (1 - stats.t.cdf(abs(t_stat_election), max(n - k, 1))))

                # Mean growth in election vs non-election periods
                elec_mask = pre_election[1:] == 1
                mean_elec_growth = float(np.mean(y[elec_mask])) if elec_mask.sum() > 0 else None
                mean_nonelec_growth = float(np.mean(y[~elec_mask])) if (~elec_mask).sum() > 0 else None

                nordhaus_result = {
                    "election_coefficient": round(float(beta[1]), 4),
                    "t_statistic": round(float(t_stat_election), 4),
                    "p_value": round(p_val_election, 4),
                    "significant_at_10pct": p_val_election < 0.10,
                    "mean_election_growth": round(mean_elec_growth, 4) if mean_elec_growth is not None else None,
                    "mean_nonelection_growth": round(mean_nonelec_growth, 4) if mean_nonelec_growth is not None else None,
                    "n_election_periods": int(elec_mask.sum()),
                    "n_observations": n,
                }

        # --- Fiscal expansion in election years ---
        fiscal_cycle = None
        if fiscal_rows and election_years:
            fiscal_dates = [r["date"] for r in fiscal_rows]
            fiscal_vals = np.array([float(r["value"]) for r in fiscal_rows])
            fiscal_years = np.array([int(str(d)[:4]) for d in fiscal_dates])
            election_set = set(election_years)

            elec_fiscal_mask = np.array([y in election_set for y in fiscal_years])
            if elec_fiscal_mask.sum() > 0 and (~elec_fiscal_mask).sum() > 0:
                mean_elec_fiscal = float(np.mean(fiscal_vals[elec_fiscal_mask]))
                mean_nonelec_fiscal = float(np.mean(fiscal_vals[~elec_fiscal_mask]))
                t_stat, p_val = stats.ttest_ind(
                    fiscal_vals[elec_fiscal_mask], fiscal_vals[~elec_fiscal_mask], equal_var=False
                )
                fiscal_cycle = {
                    "mean_election_fiscal": round(mean_elec_fiscal, 4),
                    "mean_nonelection_fiscal": round(mean_nonelec_fiscal, 4),
                    "difference": round(mean_elec_fiscal - mean_nonelec_fiscal, 4),
                    "t_statistic": round(float(t_stat), 4),
                    "p_value": round(float(p_val), 4),
                    "expansion_detected": mean_elec_fiscal > mean_nonelec_fiscal and p_val < 0.10,
                }

        # --- Hibbs partisan test ---
        # Fetch political orientation data
        partisan_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('dpi', 'vdem')
              AND (ds.name LIKE '%executive%party%' OR ds.name LIKE '%government%ideology%'
                   OR ds.name LIKE '%left%right%')
            ORDER BY dp.date
            """,
            (country,),
        )

        partisan_result = None
        if partisan_rows and len(partisan_rows) >= 5:
            part_years = np.array([int(str(r["date"])[:4]) for r in partisan_rows])
            part_vals = np.array([float(r["value"]) for r in partisan_rows])
            # Match to growth data by year
            common_years = sorted(set(years) & set(part_years))
            if len(common_years) >= 5:
                g_matched = np.array([float(growth[years == y][0]) for y in common_years if np.any(years == y)])
                p_matched = np.array([float(part_vals[part_years == y][0]) for y in common_years if np.any(part_years == y)])
                if len(g_matched) == len(p_matched) and len(g_matched) >= 5:
                    # Left dummy: value <= median (lower values = more left)
                    median_ideology = np.median(p_matched)
                    left_dummy = (p_matched <= median_ideology).astype(float)
                    if left_dummy.sum() > 0 and left_dummy.sum() < len(left_dummy):
                        slope, intercept, r_val, p_val, se = stats.linregress(left_dummy, g_matched)
                        partisan_result = {
                            "left_growth_effect": round(slope, 4),
                            "p_value": round(p_val, 4),
                            "r_squared": round(r_val ** 2, 4),
                            "ideology_median": round(float(median_ideology), 4),
                            "n_left_periods": int(left_dummy.sum()),
                            "n_right_periods": int((1 - left_dummy).sum()),
                        }

        # --- Central bank independence attenuation ---
        cbi_attenuation = None
        if cbi_score is not None:
            # CBI in [0,1]; higher = more independent
            cbi_attenuation = {
                "cbi_score": round(cbi_score, 3),
                "expected_cycle_attenuation": "strong" if cbi_score > 0.7 else "moderate" if cbi_score > 0.4 else "weak",
                "note": "Higher CBI dampens political cycle effects (Alesina-Summers)",
            }

        # --- Score ---
        # Components: election cycle strength, fiscal expansion, low CBI
        score_components = []

        if nordhaus_result:
            # Significant positive election coefficient -> higher stress
            if nordhaus_result["significant_at_10pct"] and nordhaus_result["election_coefficient"] > 0:
                score_components.append(40.0)
            elif nordhaus_result["election_coefficient"] > 0:
                score_components.append(20.0)
            else:
                score_components.append(10.0)
        else:
            score_components.append(25.0)  # No data, neutral

        if fiscal_cycle:
            if fiscal_cycle["expansion_detected"]:
                score_components.append(35.0)
            else:
                score_components.append(15.0)
        else:
            score_components.append(20.0)

        if cbi_score is not None:
            # Low CBI -> higher stress
            cbi_component = (1.0 - cbi_score) * 25.0
            score_components.append(cbi_component)
        else:
            score_components.append(12.5)

        score = float(np.clip(sum(score_components), 0, 100))

        result = {
            "score": round(score, 2),
            "country": country,
            "n_observations": len(growth),
            "years_covered": [int(years[0]), int(years[-1])],
        }

        if nordhaus_result:
            result["nordhaus_cycle"] = nordhaus_result
        if fiscal_cycle:
            result["fiscal_cycle"] = fiscal_cycle
        if partisan_result:
            result["hibbs_partisan"] = partisan_result
        if cbi_attenuation:
            result["central_bank_independence"] = cbi_attenuation
        if election_years:
            result["election_years"] = election_years

        return result
