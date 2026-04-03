"""Composite governance quality index from World Governance Indicators.

Aggregates six WGI dimensions (voice and accountability, political stability,
government effectiveness, regulatory quality, rule of law, control of
corruption) into a composite index using PCA or equal weighting. Provides
country ranking and time evolution.

Key references:
    Kaufmann, D., Kraay, A. & Mastruzzi, M. (2011). The worldwide governance
        indicators: methodology and analytical issues. Hague Journal on the
        Rule of Law, 3(2), 220-246.
    Kraay, A., Zoido-Lobaton, P. & Kaufmann, D. (1999). Governance matters.
        World Bank Policy Research Working Paper 2196.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

WGI_INDICATORS = [
    ("VA.EST", "voice_accountability"),
    ("PV.EST", "political_stability"),
    ("GE.EST", "govt_effectiveness"),
    ("RQ.EST", "regulatory_quality"),
    ("RL.EST", "rule_of_law"),
    ("CC.EST", "corruption_control"),
]


class GovernanceComposite(LayerBase):
    layer_id = "l4"
    name = "Governance Composite"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Compute composite governance index from WGI dimensions.

        Fetches all six WGI indicators, constructs a composite using PCA
        (first principal component) and equal weighting. Ranks countries
        and tracks time evolution.

        Returns dict with score, composite index, PCA loadings, dimension
        scores, rankings, and time trend.
        """
        country_iso3 = kwargs.get("country_iso3")

        # Fetch latest values for all six WGI indicators
        latest_data: dict[str, dict[str, float]] = {}  # iso -> indicator -> value
        for series_id, label in WGI_INDICATORS:
            rows = await db.fetch_all(
                """
                SELECT ds.country_iso3, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.series_id = ?
                  AND dp.value IS NOT NULL
                  AND dp.date = (
                      SELECT MAX(dp2.date) FROM data_points dp2
                      WHERE dp2.series_id = ds.id
                  )
                """,
                (series_id,),
            )
            for r in rows:
                latest_data.setdefault(r["country_iso3"], {})[label] = r["value"]

        if not latest_data:
            return {"score": 50, "results": {"error": "no WGI data"}}

        # Filter countries with all 6 indicators
        labels = [lbl for _, lbl in WGI_INDICATORS]
        complete_countries = {
            iso: data for iso, data in latest_data.items()
            if all(lbl in data for lbl in labels)
        }

        if len(complete_countries) < 10:
            # Fall back to countries with at least 4 indicators
            complete_countries = {
                iso: data for iso, data in latest_data.items()
                if sum(1 for lbl in labels if lbl in data) >= 4
            }

        if len(complete_countries) < 10:
            return {"score": 50, "results": {"error": "insufficient WGI coverage"}}

        isos = sorted(complete_countries.keys())
        n = len(isos)

        # Build data matrix (fill missing with column mean)
        data_matrix = np.zeros((n, 6))
        for i, iso in enumerate(isos):
            for j, label in enumerate(labels):
                data_matrix[i, j] = complete_countries[iso].get(label, np.nan)

        # Fill NaN with column means
        col_means = np.nanmean(data_matrix, axis=0)
        for j in range(6):
            mask = np.isnan(data_matrix[:, j])
            data_matrix[mask, j] = col_means[j]

        # PCA: standardize and extract first principal component
        means = np.mean(data_matrix, axis=0)
        stds = np.std(data_matrix, axis=0)
        stds[stds == 0] = 1
        standardized = (data_matrix - means) / stds

        # Covariance matrix and eigendecomposition
        cov_matrix = np.cov(standardized.T)
        eigenvalues, eigenvectors = np.linalg.eigh(cov_matrix)

        # Sort by eigenvalue (descending)
        idx = np.argsort(eigenvalues)[::-1]
        eigenvalues = eigenvalues[idx]
        eigenvectors = eigenvectors[:, idx]

        # First principal component
        pc1 = standardized @ eigenvectors[:, 0]
        pca_loadings = {labels[j]: float(eigenvectors[j, 0]) for j in range(6)}
        variance_explained = float(eigenvalues[0] / np.sum(eigenvalues))

        # Ensure higher PC1 = better governance (flip if needed)
        if np.corrcoef(pc1, data_matrix[:, labels.index("rule_of_law")])[0, 1] < 0:
            pc1 = -pc1
            pca_loadings = {k: -v for k, v in pca_loadings.items()}

        # Equal-weight composite
        np.mean(data_matrix, axis=0)
        ew_composite = np.mean(data_matrix, axis=1)

        # Rankings
        pca_ranking = np.argsort(np.argsort(-pc1)) + 1  # Rank 1 = best
        ew_ranking = np.argsort(np.argsort(-ew_composite)) + 1

        country_scores: dict[str, dict] = {}
        for i, iso in enumerate(isos):
            country_scores[iso] = {
                "pca_score": float(pc1[i]),
                "ew_score": float(ew_composite[i]),
                "pca_rank": int(pca_ranking[i]),
                "ew_rank": int(ew_ranking[i]),
                "dimensions": {labels[j]: float(data_matrix[i, j]) for j in range(6)},
            }

        # Time evolution for target country
        time_evolution = None
        if country_iso3:
            yearly_data: dict[str, dict[str, float]] = {}  # year -> indicator -> value
            for series_id, label in WGI_INDICATORS:
                rows = await db.fetch_all(
                    """
                    SELECT dp.date, dp.value
                    FROM data_series ds
                    JOIN data_points dp ON dp.series_id = ds.id
                    WHERE ds.series_id = ?
                      AND ds.country_iso3 = ?
                      AND dp.value IS NOT NULL
                    ORDER BY dp.date
                    """,
                    (series_id, country_iso3),
                )
                for r in rows:
                    yearly_data.setdefault(r["date"][:4], {})[label] = r["value"]

            if yearly_data:
                years = sorted(yearly_data.keys())
                avg_by_year = []
                for yr in years:
                    vals = [yearly_data[yr].get(lbl) for lbl in labels if lbl in yearly_data[yr]]
                    if vals:
                        avg_by_year.append(float(np.mean(vals)))
                    else:
                        avg_by_year.append(None)

                # Filter out None values for trend
                valid = [(y, v) for y, v in zip(years, avg_by_year) if v is not None]
                if len(valid) >= 3:
                    trend_vals = [v for _, v in valid]
                    overall_change = trend_vals[-1] - trend_vals[0]
                    time_evolution = {
                        "years": [y for y, _ in valid],
                        "scores": trend_vals,
                        "overall_change": float(overall_change),
                        "improving": overall_change > 0,
                    }

        # Governance clusters
        # Use PCA scores to identify governance tiers
        q25, q50, q75 = np.percentile(pc1, [25, 50, 75])
        clusters = {
            "weak": [iso for i, iso in enumerate(isos) if pc1[i] < q25],
            "moderate": [iso for i, iso in enumerate(isos) if q25 <= pc1[i] < q50],
            "good": [iso for i, iso in enumerate(isos) if q50 <= pc1[i] < q75],
            "strong": [iso for i, iso in enumerate(isos) if pc1[i] >= q75],
        }

        # Target country
        target = country_scores.get(country_iso3) if country_iso3 else None
        target_cluster = None
        if country_iso3:
            for cluster_name, members in clusters.items():
                if country_iso3 in members:
                    target_cluster = cluster_name
                    break

        # Score: weak governance = high score (stress)
        if target:
            ew = target["ew_score"]
            # WGI ranges roughly -2.5 to 2.5
            normalized = (ew + 2.5) / 5.0  # 0 to 1
            score = 90 - normalized * 80  # 10 to 90
        else:
            global_mean = float(np.mean(ew_composite))
            normalized = (global_mean + 2.5) / 5.0
            score = 90 - normalized * 80

        score = float(np.clip(score, 0, 100))

        ranked_list = sorted(country_scores.items(), key=lambda x: x[1]["pca_score"], reverse=True)

        results = {
            "pca": {
                "loadings": pca_loadings,
                "variance_explained": variance_explained,
                "eigenvalues": [float(e) for e in eigenvalues],
            },
            "target": target,
            "target_cluster": target_cluster,
            "time_evolution": time_evolution,
            "top_5": [(iso, cs["pca_score"]) for iso, cs in ranked_list[:5]],
            "bottom_5": [(iso, cs["pca_score"]) for iso, cs in ranked_list[-5:]],
            "clusters": {k: len(v) for k, v in clusters.items()},
            "n_countries": len(isos),
            "country_iso3": country_iso3,
        }

        return {"score": score, "results": results}
