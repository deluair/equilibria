"""Outlier detection: Z-score outlier detection in macroeconomic series.

Methodology
-----------
**Z-Score Outlier Detection**:
    z_t = (y_t - mu) / sigma
    where mu = sample mean, sigma = sample standard deviation

Observation flagged as outlier if |z_t| > threshold (default: 2.5).

A z-score of 2.5 corresponds to approximately the 99.4th percentile of a
normal distribution, so roughly 0.6% of observations would be flagged
by chance under normality.

High outlier frequency in macroeconomic series signals:
    - Data quality issues (measurement error, revisions)
    - Structural breaks or crises (legitimate but important for modeling)
    - Inconsistent data collection methodology

Series: GDP growth rate (NY.GDP.MKTP.KD.ZG)

Score = clip(outlier_fraction * 200, 0, 100)
    - 0 outliers: score 0
    - 25% outliers: score 50
    - 50%+ outliers: score 100

Also reports individual outlier years for inspection.

References:
    Barnett, V. & Lewis, T. (1994). Outliers in Statistical Data.
        3rd ed. Wiley.
"""

import numpy as np

from app.layers.base import LayerBase


class OutlierDetection(LayerBase):
    layer_id = "l18"
    name = "Outlier Detection"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        threshold = kwargs.get("z_threshold", 2.5)

        rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.indicator_code = 'NY.GDP.MKTP.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        dated = [(r["date"], float(r["value"])) for r in rows if r["value"] is not None]

        if len(dated) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient data"}

        dates, values = zip(*dated)
        y = np.array(values)
        n = len(y)

        mu = float(np.mean(y))
        sigma = float(np.std(y, ddof=1))

        if sigma < 1e-10:
            return {
                "score": 0.0,
                "country": country,
                "n_obs": n,
                "mean": round(mu, 4),
                "std": 0.0,
                "outlier_count": 0,
                "outlier_fraction": 0.0,
                "outliers": [],
                "interpretation": "Zero variance in series (all values identical)",
            }

        z_scores = (y - mu) / sigma
        outlier_mask = np.abs(z_scores) > threshold

        outlier_count = int(np.sum(outlier_mask))
        outlier_fraction = outlier_count / n

        outliers = []
        for i, flag in enumerate(outlier_mask):
            if flag:
                outliers.append({
                    "date": dates[i],
                    "value": round(float(y[i]), 4),
                    "z_score": round(float(z_scores[i]), 4),
                })

        score = float(np.clip(outlier_fraction * 200, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "mean": round(mu, 4),
            "std": round(sigma, 4),
            "threshold": threshold,
            "outlier_count": outlier_count,
            "outlier_fraction": round(outlier_fraction, 4),
            "outliers": outliers,
            "interpretation": (
                f"No outliers detected in GDP growth series (|z| <= {threshold})"
                if outlier_count == 0
                else (
                    f"{outlier_count} outlier(s) detected ({round(outlier_fraction * 100, 1)}% of obs, "
                    f"|z| > {threshold})"
                )
            ),
        }
