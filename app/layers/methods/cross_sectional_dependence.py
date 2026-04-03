"""Cross-sectional dependence: Pesaran CD test proxy via correlation with time trend.

Methodology
-----------
**Pesaran CD Test Proxy**:
In the full panel setting, the Pesaran (2004) CD statistic measures average pairwise
correlations across countries:
    CD = sqrt(2T / (N(N-1))) * sum_{i<j} rho_ij

When only a single country's data is available, we proxy cross-sectional dependence
by measuring the correlation of the country's GDP growth with a linear time trend,
treating the trend as a proxy for a common global factor (e.g., world business cycle).

High correlation with a common time trend suggests the country's growth is driven by
global factors, which, in a panel context, would manifest as cross-sectional dependence
(correlated errors across countries).

Correlation:
    rho = corr(growth_t, t)

Score = clip(rho^2 * 100, 0, 100)
    - rho^2 = 0 (no trend correlation, low CSD risk): score = 0
    - rho^2 = 1 (perfectly trend-correlated): score = 100

Additionally, a rolling window correlation is computed to detect time-varying dependence.

References:
    Pesaran, M.H. (2004). General diagnostic tests for cross section dependence in panels.
        Cambridge Working Paper in Economics 435.
    Pesaran, M.H. (2015). Testing weak cross-sectional dependence in large panels.
        Econometric Reviews 34(6-10): 1089-1117.
"""

import numpy as np
from scipy.stats import pearsonr

from app.layers.base import LayerBase


class CrossSectionalDependence(LayerBase):
    layer_id = "l18"
    name = "Cross-Sectional Dependence"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        window = kwargs.get("rolling_window", 10)

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
        t = np.arange(n, dtype=float)

        # Full-sample correlation with time trend (proxy for global common factor)
        if n > 2:
            try:
                corr, p_corr = pearsonr(y, t)
            except Exception:
                corr = float(np.corrcoef(y, t)[0, 1])
                p_corr = None
        else:
            corr = 0.0
            p_corr = None

        corr = float(corr) if not np.isnan(corr) else 0.0
        r_squared = corr ** 2

        # Rolling window correlation (detect time-varying dependence)
        rolling = []
        if n >= window:
            for i in range(n - window + 1):
                y_w = y[i: i + window]
                t_w = t[i: i + window]
                c_w = float(np.corrcoef(y_w, t_w)[0, 1])
                if not np.isnan(c_w):
                    rolling.append({
                        "start": dates[i],
                        "end": dates[i + window - 1],
                        "correlation": round(c_w, 4),
                    })

        max_rolling_abs = max((abs(r["correlation"]) for r in rolling), default=0.0)

        high_dependence = abs(corr) > 0.7 or max_rolling_abs > 0.85

        # Score: high correlation with time trend = high CSD concern
        score = float(np.clip(r_squared * 100, 0, 100))

        result = {
            "score": round(score, 2),
            "country": country,
            "n_obs": n,
            "trend_correlation": {
                "correlation": round(corr, 4),
                "r_squared": round(r_squared, 4),
                "p_value": round(float(p_corr), 4) if p_corr is not None else None,
                "high_dependence": high_dependence,
            },
            "rolling_window": window,
            "max_rolling_abs_corr": round(max_rolling_abs, 4),
            "rolling_correlations": rolling,
            "interpretation": (
                "Low cross-sectional dependence risk (weak trend correlation)"
                if not high_dependence
                else (
                    f"High cross-sectional dependence risk (trend corr={round(corr, 3)}): "
                    "growth may be driven by common global factors"
                )
            ),
        }

        return result
