"""Technology-Inequality module.

Internet adoption vs income inequality (Ragnedda & Muschert 2013;
van Dijk 2020 digital divide).

Queries internet users as % of population (IT.NET.USER.ZS) and
Gini coefficient (SI.POV.GINI). Low digital adoption combined with
high income inequality signals a reinforcing digital-economic divide.
The two dimensions compound: inequality limits adoption, low adoption
limits economic mobility.

Score rises when internet adoption is low AND Gini is high.
"""

from __future__ import annotations

import numpy as np
from scipy.stats import pearsonr

from app.layers.base import LayerBase

# Benchmarks
_INTERNET_LOW = 40.0    # <40% penetration = low adoption
_INTERNET_HIGH = 80.0   # >80% = high adoption
_GINI_HIGH = 40.0       # Gini >40 = high inequality (UN threshold)
_GINI_MODERATE = 30.0   # Gini 30-40 = moderate


class TechnologyInequality(LayerBase):
    layer_id = "lCX"
    name = "Technology-Inequality"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        rows_internet = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'IT.NET.USER.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        rows_gini = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SI.POV.GINI'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows_internet:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient internet adoption data",
            }

        internet_map = {r["date"]: float(r["value"]) for r in rows_internet if r["value"] is not None}
        gini_map = {r["date"]: float(r["value"]) for r in rows_gini if r["value"] is not None} if rows_gini else {}

        internet_dates = sorted(internet_map)
        internet_vals = np.array([internet_map[d] for d in internet_dates])
        internet_recent = float(internet_vals[-1]) if len(internet_vals) > 0 else float(np.mean(internet_vals))

        # Internet adoption stress (0-50 points): below low threshold
        if internet_recent < _INTERNET_LOW:
            adoption_stress = float(
                np.clip((_INTERNET_LOW - internet_recent) / _INTERNET_LOW * 50.0, 0.0, 50.0)
            )
        elif internet_recent < _INTERNET_HIGH:
            adoption_stress = float(
                (_INTERNET_HIGH - internet_recent) / (_INTERNET_HIGH - _INTERNET_LOW) * 20.0
            )
        else:
            adoption_stress = 0.0

        # Gini stress (0-40 points)
        gini_stress = 0.0
        gini_mean = None
        corr = None
        p_value = None

        if gini_map:
            common_dates = sorted(set(internet_map) & set(gini_map))
            if len(common_dates) >= 4:
                gini_vals = np.array([gini_map[d] for d in common_dates])
                gini_mean = float(np.mean(gini_vals))
                gini_recent = float(gini_vals[-1])

                if gini_recent > _GINI_HIGH:
                    gini_stress = float(
                        np.clip((gini_recent - _GINI_HIGH) / 20.0 * 40.0, 0.0, 40.0)
                    )
                elif gini_recent > _GINI_MODERATE:
                    gini_stress = float(
                        (gini_recent - _GINI_MODERATE) / (_GINI_HIGH - _GINI_MODERATE) * 20.0
                    )

                inet_common = np.array([internet_map[d] for d in common_dates])
                if len(common_dates) >= 6:
                    corr_val, p_val = pearsonr(inet_common, gini_vals)
                    corr = round(float(corr_val), 4)
                    p_value = round(float(p_val), 4)

        # Interaction: compound stress when both low adoption AND high inequality
        interaction_penalty = 0.0
        if gini_mean is not None and internet_recent < _INTERNET_LOW and gini_mean > _GINI_HIGH:
            interaction_penalty = 10.0

        score = float(np.clip(adoption_stress + gini_stress + interaction_penalty, 0.0, 100.0))

        result = {
            "score": round(score, 1),
            "country": country,
            "n_obs": len(internet_vals),
            "period": f"{internet_dates[0]} to {internet_dates[-1]}" if internet_dates else "unknown",
            "internet_users_recent_pct": round(internet_recent, 2),
            "internet_mean_pct": round(float(np.mean(internet_vals)), 2),
            "adoption_stress": round(adoption_stress, 2),
            "gini_stress": round(gini_stress, 2),
            "interpretation": (
                "digital inclusion gap minimal" if score < 25
                else "moderate digital divide" if score < 50
                else "severe technology-inequality trap"
            ),
            "reference": "van Dijk 2020; Ragnedda & Muschert 2013",
        }

        if gini_mean is not None:
            result["gini_mean"] = round(gini_mean, 2)
        if corr is not None:
            result["internet_gini_corr"] = corr
            result["p_value"] = p_value

        return result
