"""Cultural consumption inequality: inequality in access to arts and culture.

Cultural participation is stratified by income, geography, and education.
Higher-income households spend disproportionately on arts, entertainment, and
recreation. Proxied by the Gini coefficient (SI.POV.GINI) as the primary
income inequality signal and internet access (IT.NET.USER.ZS) as a proxy
for the digital cultural access gap -- low-income populations are excluded
from digital streaming, online arts markets, and cultural platforms.

Score: low Gini + high internet = STABLE equitable cultural access,
high Gini + low internet = CRISIS severe cultural exclusion of low-income groups.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CulturalConsumptionInequality(LayerBase):
    layer_id = "lAR"
    name = "Cultural Consumption Inequality"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gini_code = "SI.POV.GINI"
        inet_code = "IT.NET.USER.ZS"

        gini_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gini_code, "%Gini%"),
        )
        inet_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (inet_code, "%Individuals using the Internet%"),
        )

        gini_vals = [r["value"] for r in gini_rows if r["value"] is not None]
        inet_vals = [r["value"] for r in inet_rows if r["value"] is not None]

        if not gini_vals and not inet_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SI.POV.GINI or IT.NET.USER.ZS",
            }

        gini_latest = gini_vals[0] if gini_vals else None
        inet_latest = inet_vals[0] if inet_vals else None

        # Gini component: higher Gini = worse cultural inequality
        # Gini 0-100; reference: <30 = low, 30-45 = moderate, >45 = high inequality
        if gini_latest is not None:
            if gini_latest < 25.0:
                gini_score = 5.0 + gini_latest * 0.5
            elif gini_latest < 40.0:
                score_base = 17.5
                gini_score = score_base + (gini_latest - 25.0) * 2.17
            elif gini_latest < 55.0:
                score_base = 50.0
                gini_score = score_base + (gini_latest - 40.0) * 1.67
            else:
                gini_score = min(100.0, 75.05 + (gini_latest - 55.0) * 1.0)
        else:
            gini_score = 50.0  # neutral when missing

        # Internet access component: higher access = lower inequality score
        # Low access means digital exclusion worsens cultural gap
        if inet_latest is not None:
            access_penalty = max(0.0, (100.0 - inet_latest) / 100.0 * 30.0)
        else:
            access_penalty = 15.0  # moderate penalty when missing

        score = min(100.0, gini_score + access_penalty)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "gini_coefficient": round(gini_latest, 2) if gini_latest is not None else None,
                "internet_users_pct": round(inet_latest, 2) if inet_latest is not None else None,
                "gini_score_component": round(gini_score, 2),
                "access_penalty": round(access_penalty, 2),
                "n_obs_gini": len(gini_vals),
                "n_obs_internet": len(inet_vals),
            },
        }
