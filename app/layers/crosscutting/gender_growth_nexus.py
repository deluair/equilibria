"""Gender-Growth Nexus module.

Female labor force participation gap and foregone growth
(Elborgh-Woytek et al., IMF 2013).

Queries female LFPR (SL.TLF.CACT.FE.ZS) and male LFPR
(SL.TLF.CACT.MA.ZS). The gender gap in labor force participation
represents foregone output: closing the gap is estimated to raise
GDP by 10-35% in high-gap countries (IMF 2013). A large and
persistent gap signals gender-growth stress.

Score rises with the size of the gap and its persistence over time.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

# IMF (2013) benchmark: OECD average gap ~13pp; emerging market ~30pp
_GAP_LOW = 10.0    # <10pp gap = low stress
_GAP_MODERATE = 25.0  # 10-25pp = moderate
_GAP_HIGH = 40.0   # >40pp = severe


class GenderGrowthNexus(LayerBase):
    layer_id = "lCX"
    name = "Gender-Growth Nexus"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_female = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.TLF.CACT.FE.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        rows_male = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.TLF.CACT.MA.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows_female or not rows_male:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for female or male LFPR",
            }

        female_map = {r["date"]: float(r["value"]) for r in rows_female if r["value"] is not None}
        male_map = {r["date"]: float(r["value"]) for r in rows_male if r["value"] is not None}

        common_dates = sorted(set(female_map) & set(male_map))
        if len(common_dates) < 4:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"only {len(common_dates)} overlapping observations (need 4+)",
            }

        female_vals = np.array([female_map[d] for d in common_dates])
        male_vals = np.array([male_map[d] for d in common_dates])
        gaps = male_vals - female_vals  # positive = female underparticipation

        gap_mean = float(np.mean(gaps))
        gap_recent = float(gaps[-1])
        gap_trend = float(np.mean(np.diff(gaps))) if len(gaps) > 1 else 0.0

        # Gap size stress (0-70 points)
        if gap_recent > _GAP_HIGH:
            gap_score = float(np.clip(
                50.0 + (gap_recent - _GAP_HIGH) / _GAP_HIGH * 20.0, 50.0, 70.0
            ))
        elif gap_recent > _GAP_MODERATE:
            gap_score = float(
                25.0 + (gap_recent - _GAP_MODERATE) / (_GAP_HIGH - _GAP_MODERATE) * 25.0
            )
        elif gap_recent > _GAP_LOW:
            gap_score = float(
                (gap_recent - _GAP_LOW) / (_GAP_MODERATE - _GAP_LOW) * 25.0
            )
        else:
            gap_score = 0.0

        # Persistence: gap not narrowing -> additional stress (0-20 points)
        persistence_penalty = float(np.clip(gap_trend * 5.0, 0.0, 20.0)) if gap_trend > 0 else 0.0

        # Foregone GDP estimate (IMF 2013 calibration: ~0.4% GDP per 1pp gap)
        foregone_gdp_est = gap_recent * 0.4

        score = float(np.clip(gap_score + persistence_penalty, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(common_dates),
            "period": f"{common_dates[0]} to {common_dates[-1]}",
            "female_lfpr_recent": round(float(female_vals[-1]), 2),
            "male_lfpr_recent": round(float(male_vals[-1]), 2),
            "gender_gap_recent_pp": round(gap_recent, 2),
            "gender_gap_mean_pp": round(gap_mean, 2),
            "gender_gap_trend_pp_per_year": round(gap_trend, 4),
            "foregone_gdp_estimate_pct": round(foregone_gdp_est, 2),
            "gap_score": round(gap_score, 2),
            "persistence_penalty": round(persistence_penalty, 2),
            "interpretation": (
                "small gender gap" if gap_recent < _GAP_LOW
                else "moderate gender gap" if gap_recent < _GAP_MODERATE
                else "large gender participation gap"
            ),
            "reference": "Elborgh-Woytek et al. IMF SDN/13/10 (2013)",
        }
