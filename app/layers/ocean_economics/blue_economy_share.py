"""Blue economy share: fisheries, tourism, and shipping as % GDP composite.

Constructs a composite blue economy share estimate from three proxies:
- Fisheries: agriculture value added (NV.AGR.TOTL.ZS) x 0.15 (FAO fish fraction)
- Tourism/exports: exports of goods and services (NE.EXP.GNFS.ZS) x 0.05
- Shipping logistics: logistics performance overall (LP.LPI.OVRL.XQ) as quality weight

OECD (2016) projects the ocean economy at $3T by 2030 (from $1.5T in 2010).

Sources: World Bank WDI (NV.AGR.TOTL.ZS, NE.EXP.GNFS.ZS, LP.LPI.OVRL.XQ), OECD 2016
"""

from __future__ import annotations

from app.layers.base import LayerBase

# OECD 2016 baseline blue economy share of global GDP (%)
OECD_BLUE_ECONOMY_GDP_SHARE = 2.5


class BlueEconomyShare(LayerBase):
    layer_id = "lOE"
    name = "Blue Economy Share"

    async def compute(self, db, **kwargs) -> dict:
        ag_code = "NV.AGR.TOTL.ZS"
        ag_name = "agriculture value added"
        ag_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ag_code, f"%{ag_name}%"),
        )

        exp_code = "NE.EXP.GNFS.ZS"
        exp_name = "exports of goods and services"
        exp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (exp_code, f"%{exp_name}%"),
        )

        lpi_code = "LP.LPI.OVRL.XQ"
        lpi_name = "logistics performance"
        lpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (lpi_code, f"%{lpi_name}%"),
        )

        if not ag_rows and not exp_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No blue economy base data found",
            }

        ag_vals = [row["value"] for row in ag_rows if row["value"] is not None]
        exp_vals = [row["value"] for row in exp_rows if row["value"] is not None]
        lpi_vals = [row["value"] for row in lpi_rows if row["value"] is not None]

        ag_latest = float(ag_vals[0]) if ag_vals else None
        exp_latest = float(exp_vals[0]) if exp_vals else None
        lpi_latest = float(lpi_vals[0]) if lpi_vals else None

        if ag_latest is None and exp_latest is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All blue economy rows have null values",
            }

        # Component estimates
        fish_pct = (ag_latest * 0.15) if ag_latest is not None else 1.0
        shipping_pct = (exp_latest * 0.05) if exp_latest is not None else 1.0
        # LPI quality multiplier: better logistics lifts effective blue economy contribution
        lpi_multiplier = (lpi_latest / 5.0) if lpi_latest is not None else 0.6
        tourism_pct = 1.5 * lpi_multiplier

        total_blue_pct = fish_pct + shipping_pct + tourism_pct

        # Score: compare to OECD global baseline
        # Below baseline = score rises (underdeveloped blue economy is a structural gap)
        ratio = total_blue_pct / OECD_BLUE_ECONOMY_GDP_SHARE
        if ratio < 0.5:
            score = 70.0  # very low blue economy share = underdevelopment risk
        elif ratio < 1.0:
            score = 45.0
        elif ratio < 2.0:
            score = 25.0
        else:
            score = 15.0  # strong blue economy = lower structural risk

        score = round(min(100.0, score), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "fisheries_pct_gdp_est": round(fish_pct, 3),
                "shipping_pct_gdp_est": round(shipping_pct, 3),
                "tourism_pct_gdp_est": round(tourism_pct, 3),
                "total_blue_economy_pct_gdp": round(total_blue_pct, 3),
                "oecd_global_baseline_pct": OECD_BLUE_ECONOMY_GDP_SHARE,
                "ratio_to_oecd_baseline": round(ratio, 3),
                "lpi_quality_multiplier": round(lpi_multiplier, 3),
                "n_ag_obs": len(ag_vals),
                "n_exp_obs": len(exp_vals),
                "n_lpi_obs": len(lpi_vals),
            },
        }
