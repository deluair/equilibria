"""Inequality decomposition using Theil index and generalized entropy.

Decomposes total inequality into within-group and between-group components
using the Theil index (GE(1)) and other generalized entropy measures.
Also computes Gini decomposition by income source.

Key references:
    Theil, H. (1967). Economics and Information Theory. North-Holland.
    Shorrocks, A. (1980). The class of additively decomposable inequality
        measures. Econometrica, 48(3), 613-625.
    Cowell, F. (2011). Measuring Inequality (3rd ed.). Oxford University Press.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


def _gini(x: np.ndarray) -> float:
    """Compute Gini coefficient from an array of values."""
    if len(x) < 2:
        return 0.0
    x = np.sort(x)
    n = len(x)
    cumx = np.cumsum(x)
    return float((2 * np.sum((np.arange(1, n + 1) * x)) - (n + 1) * cumx[-1]) / (n * cumx[-1]))


def _generalized_entropy(x: np.ndarray, alpha: float) -> float:
    """Compute generalized entropy index GE(alpha).

    alpha=0: Mean Log Deviation (Theil L)
    alpha=1: Theil T index
    alpha=2: Half the squared coefficient of variation
    """
    x = x[x > 0]
    if len(x) < 2:
        return 0.0
    mu = np.mean(x)
    len(x)

    if alpha == 0:
        return float(np.mean(np.log(mu / x)))
    elif alpha == 1:
        ratios = x / mu
        return float(np.mean(ratios * np.log(ratios)))
    else:
        return float(np.mean((x / mu) ** alpha - 1) / (alpha * (alpha - 1)))


def _theil_decomposition(
    values: np.ndarray, groups: np.ndarray
) -> dict:
    """Decompose Theil T index into within and between group components.

    Returns dict with total, within, between, and group-level details.
    """
    unique_groups = np.unique(groups)
    n_total = len(values)
    mu_total = np.mean(values)
    total_theil = _generalized_entropy(values, 1)

    within = 0.0
    between = 0.0
    group_details = {}

    for g in unique_groups:
        mask = groups == g
        x_g = values[mask]
        n_g = len(x_g)
        mu_g = np.mean(x_g)

        if n_g < 2 or mu_g <= 0:
            continue

        # Population share
        s_g = n_g / n_total
        # Income share
        w_g = (n_g * mu_g) / (n_total * mu_total)

        # Within component: w_g * GE(1)_g
        theil_g = _generalized_entropy(x_g, 1)
        within += w_g * theil_g

        # Between component
        between += w_g * np.log(mu_g / mu_total)

        group_details[str(g)] = {
            "n": int(n_g),
            "mean": float(mu_g),
            "pop_share": float(s_g),
            "income_share": float(w_g),
            "theil": float(theil_g),
            "gini": float(_gini(x_g)),
        }

    return {
        "total_theil": float(total_theil),
        "within": float(within),
        "between": float(between),
        "within_share": float(within / total_theil) if total_theil > 0 else 0,
        "between_share": float(between / total_theil) if total_theil > 0 else 0,
        "groups": group_details,
    }


class InequalityDecomposition(LayerBase):
    layer_id = "l4"
    name = "Inequality Decomposition"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Decompose inequality using Theil index and generalized entropy.

        Fetches cross-country GDP per capita data and decomposes total
        inequality into within-region and between-region components.
        Computes multiple GE indices for robustness.

        Returns dict with score, Theil decomposition, GE indices,
        Gini coefficient, and regional contributions.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Fetch latest GDP per capita
        gdp_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
              AND dp.value > 0
              AND dp.date = (
                  SELECT MAX(dp2.date) FROM data_points dp2
                  WHERE dp2.series_id = ds.id
              )
            """
        )

        # Fetch country regions for decomposition
        region_rows = await db.fetch_all(
            "SELECT iso3, region, income_group FROM countries WHERE region IS NOT NULL"
        )

        if not gdp_rows:
            return {"score": 50, "results": {"error": "no GDP per capita data"}}

        gdp_dict = {r["country_iso3"]: r["value"] for r in gdp_rows}
        region_dict = {r["iso3"]: r["region"] for r in region_rows} if region_rows else {}
        income_dict = {r["iso3"]: r["income_group"] for r in region_rows} if region_rows else {}

        values = np.array(list(gdp_dict.values()))

        # Global inequality measures
        gini = _gini(values)
        ge0 = _generalized_entropy(values, 0)  # Mean log deviation
        ge1 = _generalized_entropy(values, 1)  # Theil T
        ge2 = _generalized_entropy(values, 2)  # Half CV^2
        cv = float(np.std(values) / np.mean(values))
        p90_p10 = float(np.percentile(values, 90) / np.percentile(values, 10))

        # Regional decomposition
        regional_decomp = None
        if region_dict:
            common = [iso for iso in gdp_dict if iso in region_dict]
            if len(common) >= 20:
                vals = np.array([gdp_dict[c] for c in common])
                groups = np.array([region_dict[c] for c in common])
                regional_decomp = _theil_decomposition(vals, groups)

        # Income group decomposition
        income_decomp = None
        if income_dict:
            common = [iso for iso in gdp_dict if iso in income_dict]
            if len(common) >= 20:
                vals = np.array([gdp_dict[c] for c in common])
                groups = np.array([income_dict[c] for c in common])
                income_decomp = _theil_decomposition(vals, groups)

        # Time trend in global inequality
        trend_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.country_iso3
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'NY.GDP.PCAP.KD'
              AND dp.value > 0
            ORDER BY dp.date
            """
        )
        inequality_trend = None
        if trend_rows:
            by_year: dict[str, list[float]] = {}
            for r in trend_rows:
                by_year.setdefault(r["date"][:4], []).append(r["value"])

            trend_years = sorted(y for y, v in by_year.items() if len(v) >= 20)
            if len(trend_years) >= 5:
                gini_series = [_gini(np.array(by_year[y])) for y in trend_years]
                theil_series = [_generalized_entropy(np.array(by_year[y]), 1) for y in trend_years]
                inequality_trend = {
                    "years": trend_years,
                    "gini": gini_series,
                    "theil": theil_series,
                    "gini_change": gini_series[-1] - gini_series[0],
                    "declining": gini_series[-1] < gini_series[0],
                }

        # Target country percentile
        target_percentile = None
        if country_iso3 and country_iso3 in gdp_dict:
            target_val = gdp_dict[country_iso3]
            target_percentile = float(np.mean(values <= target_val) * 100)

        # Score: high global inequality = stress
        if gini > 0.6:
            score = 80
        elif gini > 0.5:
            score = 65
        elif gini > 0.4:
            score = 50
        elif gini > 0.3:
            score = 35
        else:
            score = 20

        score = float(np.clip(score, 0, 100))

        results = {
            "global": {
                "gini": gini,
                "theil_t": ge1,
                "mean_log_deviation": ge0,
                "half_cv_sq": ge2,
                "cv": cv,
                "p90_p10": p90_p10,
                "mean_gdppc": float(np.mean(values)),
                "median_gdppc": float(np.median(values)),
                "n_countries": len(values),
            },
            "regional_decomposition": regional_decomp,
            "income_group_decomposition": income_decomp,
            "trend": inequality_trend,
            "target_percentile": target_percentile,
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
