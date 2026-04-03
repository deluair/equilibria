"""Financial Conditions Index - PCA-based composite from 12 financial indicators."""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class FinancialConditionsIndex(LayerBase):
    layer_id = "l2"
    name = "Financial Conditions Index"
    weight = 0.05

    # 12 component series (FRED mnemonics)
    COMPONENTS = [
        "BAMLC0A0CM",    # ICE BofA US Corporate Index OAS (credit spread)
        "BAMLH0A0HYM2",  # ICE BofA US High Yield OAS
        "TEDRATE",       # TED Spread
        "T10Y2Y",        # 10Y-2Y Treasury term spread
        "T10YIE",        # 10-Year breakeven inflation
        "VIXCLS",        # CBOE VIX
        "DTWEXBGS",      # Trade-weighted USD index
        "SP500",         # S&P 500 level
        "MORTGAGE30US",  # 30-Year fixed mortgage rate
        "FEDFUNDS",      # Fed Funds effective rate
        "M2SL",          # M2 money supply
        "TOTALSL",       # Total consumer credit
    ]

    TIGHTENING_SIGN = {
        "BAMLC0A0CM": 1, "BAMLH0A0HYM2": 1, "TEDRATE": 1,
        "T10Y2Y": -1, "T10YIE": -1, "VIXCLS": 1,
        "DTWEXBGS": 1, "SP500": -1, "MORTGAGE30US": 1,
        "FEDFUNDS": 1, "M2SL": -1, "TOTALSL": -1,
    }

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 10)

        rows = await db.execute_fetchall(
            """
            SELECT series_id, date, value FROM data_points
            WHERE series_id IN ({})
              AND country_code = ?
              AND date >= date('now', ?)
            ORDER BY series_id, date
            """.format(",".join("?" for _ in self.COMPONENTS)),
            (*self.COMPONENTS, country, f"-{lookback} years"),
        )

        series_map: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            series_map.setdefault(r["series_id"], []).append(
                (r["date"], float(r["value"]))
            )

        if len(series_map) < 3:
            return {"score": 50.0, "results": {}, "note": "insufficient data"}

        # Align to common dates
        all_dates = set()
        for pts in series_map.values():
            all_dates.update(d for d, _ in pts)
        common_dates = sorted(all_dates)

        date_idx = {d: i for i, d in enumerate(common_dates)}
        n_dates = len(common_dates)
        n_series = len(series_map)
        matrix = np.full((n_dates, n_series), np.nan)

        series_ids = list(series_map.keys())
        for col, sid in enumerate(series_ids):
            for d, v in series_map[sid]:
                matrix[date_idx[d], col] = v

        # Forward-fill then drop remaining NaN rows
        for col in range(n_series):
            last = np.nan
            for row in range(n_dates):
                if np.isnan(matrix[row, col]):
                    matrix[row, col] = last
                else:
                    last = matrix[row, col]

        valid_mask = ~np.any(np.isnan(matrix), axis=1)
        matrix = matrix[valid_mask]
        valid_dates = [d for d, m in zip(common_dates, valid_mask) if m]

        if matrix.shape[0] < 12:
            return {"score": 50.0, "results": {}, "note": "insufficient aligned data"}

        # Standardize
        means = np.mean(matrix, axis=0)
        stds = np.std(matrix, axis=0, ddof=1)
        stds[stds < 1e-12] = 1.0
        Z = (matrix - means) / stds

        # Flip sign so positive = tightening for all components
        for col, sid in enumerate(series_ids):
            sign = self.TIGHTENING_SIGN.get(sid, 1)
            Z[:, col] *= sign

        # PCA via eigendecomposition of correlation matrix
        corr = np.corrcoef(Z, rowvar=False)
        eigenvalues, eigenvectors = np.linalg.eigh(corr)
        # Sort descending
        idx_sort = np.argsort(eigenvalues)[::-1]
        eigenvalues = eigenvalues[idx_sort]
        eigenvectors = eigenvectors[:, idx_sort]

        # First principal component = FCI
        pc1 = Z @ eigenvectors[:, 0]
        variance_explained = float(eigenvalues[0] / np.sum(eigenvalues))

        # Component loadings
        loadings = {
            sid: float(eigenvectors[col, 0])
            for col, sid in enumerate(series_ids)
        }

        # Current FCI value and percentile rank in history
        current_fci = float(pc1[-1])
        percentile = float(sp_stats.percentileofscore(pc1, current_fci))

        # FCI time series (last 60 observations)
        fci_ts = [
            {"date": valid_dates[-min(60, len(pc1)) + i], "value": float(pc1[-min(60, len(pc1)) + i])}
            for i in range(min(60, len(pc1)))
        ]

        # Score: map percentile to 0-100 stress scale
        # High FCI percentile = tight conditions = higher stress score
        score = float(np.clip(percentile, 0, 100))

        return {
            "score": score,
            "results": {
                "fci_current": current_fci,
                "fci_percentile": percentile,
                "variance_explained_pc1": variance_explained,
                "n_components_used": n_series,
                "loadings": loadings,
                "eigenvalues": [float(e) for e in eigenvalues[:5]],
                "fci_series": fci_ts,
                "z_scores_latest": {
                    sid: float(Z[-1, col])
                    for col, sid in enumerate(series_ids)
                },
            },
        }
