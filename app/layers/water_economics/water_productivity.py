"""Water productivity: GDP output per unit of freshwater withdrawn.

Proxied as NY.GDP.MKTP.KD (constant GDP, USD) relative to ER.H2O.FWTL.ZS
(freshwater withdrawal % of internal resources). Lower productivity signals
inefficient water use relative to economic output.

Sources: World Bank WDI (NY.GDP.MKTP.KD, ER.H2O.FWTL.ZS)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class WaterProductivity(LayerBase):
    layer_id = "lWA"
    name = "Water Productivity"

    async def compute(self, db, **kwargs) -> dict:
        gdp_code = "NY.GDP.MKTP.KD"
        gdp_name = "GDP constant"
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gdp_code, f"%{gdp_name}%"),
        )

        withdrawal_code = "ER.H2O.FWTL.ZS"
        withdrawal_name = "freshwater withdrawals"
        withdrawal_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (withdrawal_code, f"%{withdrawal_name}%"),
        )

        gdp_vals = [row["value"] for row in gdp_rows if row["value"] is not None]
        withdrawal_vals = [row["value"] for row in withdrawal_rows if row["value"] is not None]

        if not gdp_vals or not withdrawal_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "Insufficient GDP or freshwater withdrawal data",
            }

        gdp_latest = float(gdp_vals[0])
        withdrawal_latest = float(withdrawal_vals[0])

        if withdrawal_latest <= 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "Withdrawal rate is zero or negative",
            }

        # GDP per withdrawal unit (ratio); normalize against a benchmark
        # A higher ratio = better productivity = lower risk
        ratio = gdp_latest / withdrawal_latest

        # Normalize: use log scale. ratio > 1e12 is high productivity
        import math
        log_ratio = math.log10(ratio) if ratio > 0 else 0
        # Typical range ~8-14 in log10; map to 0-100 risk (inverse)
        normalized = min(max((log_ratio - 8.0) / 6.0, 0.0), 1.0)
        score = round((1.0 - normalized) * 100.0, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "gdp_constant_usd": round(gdp_latest, 0),
                "withdrawal_pct": round(withdrawal_latest, 2),
                "gdp_per_withdrawal_unit": round(ratio, 2),
                "n_gdp_obs": len(gdp_vals),
                "n_withdrawal_obs": len(withdrawal_vals),
            },
        }
