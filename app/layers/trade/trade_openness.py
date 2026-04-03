"""Trade openness and integration measures.

Trade Openness Ratio:
    Openness = (X + M) / GDP

The most common measure of a country's integration into the world economy.
Values range from near 0 (autarky) to well above 1.0 (entrepot economies
like Singapore or Hong Kong).

Additional measures:
- Trade intensity index: bilateral trade relative to expected share
    TII_ij = (X_ij / X_i) / (M_j / M_w)
- Marginal openness: change in trade ratio over time
- Export/import ratios to GDP separately
- Real vs nominal openness comparison

The score reflects integration risk: very high openness (>1.0) or very low
openness (<0.2) or rapid changes signal potential vulnerability to external
shocks or isolation from global supply chains.
"""

import numpy as np

from app.layers.base import LayerBase


class TradeOpenness(LayerBase):
    layer_id = "l1"
    name = "Trade Openness"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'imf', 'fred')
              AND (ds.name LIKE '%export%good%service%'
                   OR ds.name LIKE '%import%good%service%'
                   OR ds.name LIKE '%gdp%current%usd%'
                   OR ds.name LIKE '%trade%gdp%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no trade/GDP data"}

        exports: dict[str, float] = {}
        imports: dict[str, float] = {}
        gdp: dict[str, float] = {}
        direct_openness: dict[str, float] = {}

        for row in rows:
            name = row["name"].lower()
            date = row["date"]
            val = row["value"]
            if val is None or val <= 0:
                continue
            if "trade" in name and "gdp" in name:
                direct_openness[date] = val
            elif "export" in name:
                exports[date] = val
            elif "import" in name:
                imports[date] = val
            elif "gdp" in name:
                gdp[date] = val

        # Compute openness ratio
        openness_series: dict[str, float] = {}
        if direct_openness:
            openness_series = direct_openness
        elif exports and imports and gdp:
            common = sorted(set(exports.keys()) & set(imports.keys()) & set(gdp.keys()))
            for d in common:
                if gdp[d] > 0:
                    openness_series[d] = (exports[d] + imports[d]) / gdp[d]

        if not openness_series:
            return {"score": None, "signal": "UNAVAILABLE", "error": "cannot compute openness"}

        dates = sorted(openness_series.keys())
        values = np.array([openness_series[d] for d in dates])

        latest = float(values[-1])
        mean_openness = float(np.mean(values))

        # Normalize if reported as percentage (>2 likely means pct)
        if latest > 2.0:
            values = values / 100.0
            latest = latest / 100.0
            mean_openness = mean_openness / 100.0

        # Trend
        t = np.arange(len(values), dtype=float)
        X = np.column_stack([np.ones(len(t)), t])
        beta = np.linalg.lstsq(X, values, rcond=None)[0]
        trend_slope = float(beta[1])

        # Marginal openness: change in (X+M) / change in GDP
        marginal_openness = None
        if exports and imports and gdp:
            common = sorted(set(exports.keys()) & set(imports.keys()) & set(gdp.keys()))
            if len(common) >= 2:
                trade_vals = np.array([exports[d] + imports[d] for d in common])
                gdp_vals = np.array([gdp[d] for d in common])
                d_trade = np.diff(trade_vals)
                d_gdp = np.diff(gdp_vals)
                mask = np.abs(d_gdp) > 0
                if np.sum(mask) > 0:
                    mo = float(np.mean(d_trade[mask] / d_gdp[mask]))
                    marginal_openness = round(mo, 4)

        # Export and import ratios separately
        export_ratio = None
        import_ratio = None
        if exports and gdp:
            common = sorted(set(exports.keys()) & set(gdp.keys()))
            if common:
                latest_d = common[-1]
                if gdp[latest_d] > 0:
                    export_ratio = round(exports[latest_d] / gdp[latest_d], 4)
        if imports and gdp:
            common = sorted(set(imports.keys()) & set(gdp.keys()))
            if common:
                latest_d = common[-1]
                if gdp[latest_d] > 0:
                    import_ratio = round(imports[latest_d] / gdp[latest_d], 4)

        # Trade balance ratio
        trade_balance_ratio = None
        if export_ratio is not None and import_ratio is not None:
            trade_balance_ratio = round(export_ratio - import_ratio, 4)

        # Score: extreme openness (very high or very low) = higher stress
        # Optimal range roughly 0.3-0.8 for diversified economies
        if latest < 0.15:
            score = 60.0 + (0.15 - latest) * 200.0  # Too closed
        elif latest > 1.0:
            score = 60.0 + (latest - 1.0) * 100.0  # Extremely open
        elif latest > 0.8:
            score = 30.0 + (latest - 0.8) * 150.0  # High exposure
        else:
            score = max(5.0, 30.0 - (latest - 0.15) * 30.0)  # Normal range

        # Volatility penalty
        if len(values) > 1:
            returns = np.diff(values) / np.maximum(values[:-1], 1e-10)
            vol = float(np.std(returns))
            score += vol * 50.0

        score = max(0.0, min(100.0, score))

        result = {
            "score": round(score, 2),
            "country": country,
            "latest_date": dates[-1],
            "openness_ratio": round(latest, 4),
            "mean_openness": round(mean_openness, 4),
            "trend_per_period": round(trend_slope, 6),
            "trend_direction": "increasing" if trend_slope > 0 else "decreasing",
            "n_observations": len(values),
            "date_range": [dates[0], dates[-1]],
            "classification": self._classify_openness(latest),
        }

        if export_ratio is not None:
            result["export_to_gdp"] = export_ratio
        if import_ratio is not None:
            result["import_to_gdp"] = import_ratio
        if trade_balance_ratio is not None:
            result["trade_balance_to_gdp"] = trade_balance_ratio
        if marginal_openness is not None:
            result["marginal_openness"] = marginal_openness

        return result

    @staticmethod
    def _classify_openness(ratio: float) -> str:
        if ratio < 0.2:
            return "relatively closed"
        elif ratio < 0.4:
            return "moderately open"
        elif ratio < 0.7:
            return "open"
        elif ratio < 1.0:
            return "highly open"
        else:
            return "entrepot/extremely open"
