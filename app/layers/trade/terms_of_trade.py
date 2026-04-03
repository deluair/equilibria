"""Terms of trade computation and decomposition.

Net Barter Terms of Trade (NBTT):
    TOT = (P_x / P_m) * 100

where P_x is the export price index and P_m is the import price index.
An increase means the country can buy more imports per unit of exports
(improvement).  A decline means deterioration.

Income Terms of Trade (ITT):
    ITT = TOT * Q_x

where Q_x is the export volume index.  Captures the combined effect of
price movements and export volume changes on purchasing power.

Decomposition of TOT changes:
    dTOT/TOT = dP_x/P_x - dP_m/P_m

This separates whether TOT movements are driven by export price changes
(e.g., commodity boom) or import price changes (e.g., oil shock).

The Prebisch-Singer hypothesis suggests LDC commodity exporters face
secular TOT decline.  The score reflects TOT stress: declining TOT
and high volatility push the score higher.
"""

import numpy as np
from app.layers.base import LayerBase


class TermsOfTrade(LayerBase):
    layer_id = "l1"
    name = "Terms of Trade"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Fetch export and import price indices
        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.name
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('wdi', 'imf', 'fred')
              AND (ds.name LIKE '%export%price%index%'
                   OR ds.name LIKE '%import%price%index%'
                   OR ds.name LIKE '%export%volume%index%'
                   OR ds.name LIKE '%terms%trade%')
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no price index data"}

        # Organize by series
        export_price: dict[str, float] = {}
        import_price: dict[str, float] = {}
        export_volume: dict[str, float] = {}
        direct_tot: dict[str, float] = {}

        for row in rows:
            name = row["name"].lower()
            date = row["date"]
            val = row["value"]
            if val is None:
                continue
            if "terms" in name and "trade" in name:
                direct_tot[date] = val
            elif "export" in name and "price" in name:
                export_price[date] = val
            elif "import" in name and "price" in name:
                import_price[date] = val
            elif "export" in name and "volume" in name:
                export_volume[date] = val

        # Use direct TOT if available, otherwise compute from price indices
        tot_series: dict[str, float] = {}
        if direct_tot:
            tot_series = direct_tot
        elif export_price and import_price:
            common = sorted(set(export_price.keys()) & set(import_price.keys()))
            for d in common:
                if import_price[d] > 0:
                    tot_series[d] = (export_price[d] / import_price[d]) * 100.0
        else:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient price data"}

        if len(tot_series) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "too few TOT observations"}

        dates = sorted(tot_series.keys())
        tot_values = np.array([tot_series[d] for d in dates])

        # Level analysis
        latest = tot_values[-1]
        mean_tot = float(np.mean(tot_values))
        std_tot = float(np.std(tot_values))

        # Trend: OLS on time index
        t = np.arange(len(tot_values), dtype=float)
        X = np.column_stack([np.ones(len(t)), t])
        beta = np.linalg.lstsq(X, tot_values, rcond=None)[0]
        trend_slope = float(beta[1])
        # Annualize if needed
        annual_trend_pct = trend_slope / mean_tot * 100.0 if mean_tot > 0 else 0.0

        # Decomposition: export vs import price contribution
        decomposition = None
        if export_price and import_price:
            common = sorted(set(export_price.keys()) & set(import_price.keys()))
            if len(common) >= 3:
                px = np.array([export_price[d] for d in common])
                pm = np.array([import_price[d] for d in common])
                # Log changes
                if len(px) > 1:
                    dpx = np.diff(np.log(np.maximum(px, 1e-10)))
                    dpm = np.diff(np.log(np.maximum(pm, 1e-10)))
                    decomposition = {
                        "export_price_contribution": round(float(np.mean(dpx)) * 100, 4),
                        "import_price_contribution": round(float(-np.mean(dpm)) * 100, 4),
                        "net_change": round(float(np.mean(dpx) - np.mean(dpm)) * 100, 4),
                    }

        # Income terms of trade
        income_tot = None
        if export_volume and tot_series:
            common = sorted(set(export_volume.keys()) & set(tot_series.keys()))
            if len(common) >= 3:
                itt = np.array([tot_series[d] * export_volume[d] / 100.0 for d in common])
                income_tot = {
                    "latest": round(float(itt[-1]), 2),
                    "mean": round(float(np.mean(itt)), 2),
                    "trend": "improving" if itt[-1] > np.mean(itt) else "declining",
                }

        # Volatility
        if len(tot_values) > 1:
            returns = np.diff(tot_values) / tot_values[:-1]
            volatility = float(np.std(returns))
        else:
            volatility = 0.0

        # Score: declining TOT + high volatility = high stress
        # Negative trend contributes to score
        trend_component = max(0.0, min(50.0, -annual_trend_pct * 10.0 + 25.0))
        # High volatility contributes to score
        vol_component = max(0.0, min(50.0, volatility * 200.0))
        score = trend_component + vol_component
        score = max(0.0, min(100.0, score))

        result = {
            "score": round(score, 2),
            "country": country,
            "latest_date": dates[-1],
            "latest_tot": round(float(latest), 2),
            "mean_tot": round(mean_tot, 2),
            "std_tot": round(std_tot, 2),
            "trend_slope_per_period": round(trend_slope, 4),
            "annual_trend_pct": round(annual_trend_pct, 4),
            "volatility": round(volatility, 4),
            "n_observations": len(tot_values),
            "date_range": [dates[0], dates[-1]],
            "trend_direction": "improving" if trend_slope > 0 else "deteriorating",
        }

        if decomposition:
            result["decomposition"] = decomposition
        if income_tot:
            result["income_terms_of_trade"] = income_tot

        return result
