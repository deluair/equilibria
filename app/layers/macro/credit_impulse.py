"""Credit Impulse - Change in credit flow as share of GDP. Leading indicator of demand."""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CreditImpulse(LayerBase):
    layer_id = "l2"
    name = "Credit Impulse"
    weight = 0.05

    # FRED series for credit aggregates and GDP
    CREDIT_SERIES = [
        "TOTBKCR",   # Bank credit, all commercial banks
        "BUSLOANS",  # Commercial and industrial loans
        "CONSUMER",  # Consumer loans
        "REALLN",    # Real estate loans
    ]
    GDP_SERIES = "GDP"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 15)

        # Fetch credit data
        credit_rows = await db.execute_fetchall(
            """
            SELECT series_id, date, value FROM data_points
            WHERE series_id IN ({})
              AND country_code = ?
              AND date >= date('now', ?)
            ORDER BY series_id, date
            """.format(",".join("?" for _ in self.CREDIT_SERIES)),
            (*self.CREDIT_SERIES, country, f"-{lookback} years"),
        )

        # Fetch GDP data
        gdp_rows = await db.execute_fetchall(
            """
            SELECT date, value FROM data_points
            WHERE series_id = ? AND country_code = ?
              AND date >= date('now', ?)
            ORDER BY date
            """,
            (self.GDP_SERIES, country, f"-{lookback} years"),
        )

        credit_map: dict[str, dict[str, float]] = {}
        for r in credit_rows:
            credit_map.setdefault(r["series_id"], {})[r["date"]] = float(r["value"])

        gdp_map = {r["date"]: float(r["value"]) for r in gdp_rows}

        if not gdp_map or not credit_map:
            return {"score": 50.0, "results": {}, "note": "insufficient data"}

        # Compute total credit stock per date
        all_dates = sorted(set(gdp_map.keys()))
        total_credit: dict[str, float] = {}
        for d in all_dates:
            credit_sum = 0.0
            count = 0
            for sid in self.CREDIT_SERIES:
                if d in credit_map.get(sid, {}):
                    credit_sum += credit_map[sid][d]
                    count += 1
            if count > 0:
                total_credit[d] = credit_sum

        common_dates = sorted(d for d in all_dates if d in total_credit and d in gdp_map)
        if len(common_dates) < 8:
            return {"score": 50.0, "results": {}, "note": "insufficient aligned data"}

        credit_arr = np.array([total_credit[d] for d in common_dates])
        gdp_arr = np.array([gdp_map[d] for d in common_dates])

        # Credit flow = change in credit stock (first difference)
        credit_flow = np.diff(credit_arr)
        gdp_mid = gdp_arr[1:]  # align with flow

        # Credit flow as % of GDP
        credit_flow_pct = (credit_flow / gdp_mid) * 100.0

        # Credit impulse = change in credit flow / GDP
        # (second difference of credit stock, normalized by GDP)
        credit_impulse = np.diff(credit_flow_pct)
        impulse_dates = common_dates[2:]

        if len(credit_impulse) < 4:
            return {"score": 50.0, "results": {}, "note": "insufficient data for impulse"}

        current_impulse = float(credit_impulse[-1])
        mean_impulse = float(np.mean(credit_impulse))
        std_impulse = float(np.std(credit_impulse, ddof=1))

        # Z-score of current impulse
        z_score = (current_impulse - mean_impulse) / std_impulse if std_impulse > 1e-12 else 0.0

        # 4-quarter moving average
        if len(credit_impulse) >= 4:
            ma4 = float(np.mean(credit_impulse[-4:]))
        else:
            ma4 = current_impulse

        # Cumulative impulse over last 4 quarters
        cum_impulse_4q = float(np.sum(credit_impulse[-4:])) if len(credit_impulse) >= 4 else current_impulse

        # Credit-to-GDP ratio
        credit_gdp_ratio = float(credit_arr[-1] / gdp_arr[-1] * 100.0) if gdp_arr[-1] != 0 else 0.0

        # Credit-to-GDP gap (deviation from HP trend)
        credit_gdp_series = (credit_arr / gdp_arr) * 100.0
        credit_gdp_gap = self._hp_gap(credit_gdp_series, lam=1600)

        # Impulse time series (last 40 observations)
        ts_len = min(40, len(credit_impulse))
        impulse_ts = [
            {"date": impulse_dates[-ts_len + i], "value": float(credit_impulse[-ts_len + i])}
            for i in range(ts_len)
        ]

        # Score: negative impulse (credit contraction) = higher stress
        # Map z-score to 0-100 scale, centered at 50
        # Negative impulse -> high score (stress), positive -> low score (expansionary)
        score = float(np.clip(50.0 - z_score * 15.0, 0, 100))

        return {
            "score": score,
            "results": {
                "current_impulse": current_impulse,
                "impulse_ma4": ma4,
                "cumulative_4q": cum_impulse_4q,
                "z_score": z_score,
                "credit_gdp_ratio": credit_gdp_ratio,
                "credit_gdp_gap": float(credit_gdp_gap) if credit_gdp_gap is not None else None,
                "mean_impulse": mean_impulse,
                "std_impulse": std_impulse,
                "impulse_series": impulse_ts,
                "n_observations": len(credit_impulse),
            },
        }

    @staticmethod
    def _hp_gap(series: np.ndarray, lam: float = 1600) -> float | None:
        """Hodrick-Prescott filter gap (last observation)."""
        n = len(series)
        if n < 8:
            return None

        # Construct HP filter matrix: (I + lambda * K'K)^{-1} y
        # K is the second-difference matrix
        e = np.eye(n)
        K = np.zeros((n - 2, n))
        for i in range(n - 2):
            K[i, i] = 1
            K[i, i + 1] = -2
            K[i, i + 2] = 1

        trend = np.linalg.solve(e + lam * K.T @ K, series)
        gap = series - trend
        return float(gap[-1])
