"""Shipping cost burden: import and export transport cost as % of trade value.

IC.IMP.COST.CD (cost to import, USD per container) and IC.EXP.COST.CD
(cost to export, USD per container) capture the direct financial drag of
maritime logistics on international competitiveness.

Sources: World Bank Doing Business (IC.IMP.COST.CD, IC.EXP.COST.CD)
"""

from __future__ import annotations

from app.layers.base import LayerBase

# Global median reference costs (World Bank Doing Business 2020 estimate)
MEDIAN_IMPORT_COST_USD = 500.0
MEDIAN_EXPORT_COST_USD = 450.0


class ShippingCostBurden(LayerBase):
    layer_id = "lOE"
    name = "Shipping Cost Burden"

    async def compute(self, db, **kwargs) -> dict:
        imp_code = "IC.IMP.COST.CD"
        imp_name = "cost to import"
        imp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (imp_code, f"%{imp_name}%"),
        )

        exp_code = "IC.EXP.COST.CD"
        exp_name = "cost to export"
        exp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (exp_code, f"%{exp_name}%"),
        )

        if not imp_rows and not exp_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No shipping cost data found",
            }

        imp_vals = [row["value"] for row in imp_rows if row["value"] is not None]
        exp_vals = [row["value"] for row in exp_rows if row["value"] is not None]

        imp_latest = float(imp_vals[0]) if imp_vals else None
        exp_latest = float(exp_vals[0]) if exp_vals else None

        if imp_latest is None and exp_latest is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All shipping cost rows have null values",
            }

        # Normalise relative to global median
        imp_ratio = (imp_latest / MEDIAN_IMPORT_COST_USD) if imp_latest else 1.0
        exp_ratio = (exp_latest / MEDIAN_EXPORT_COST_USD) if exp_latest else 1.0
        combined_ratio = (imp_ratio + exp_ratio) / 2.0

        # Score: >3x median = crisis (score ~90); at median = score ~25
        score = round(min(100.0, max(0.0, (combined_ratio - 0.5) / 3.0 * 100.0)), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "import_cost_usd_per_container": round(imp_latest, 2) if imp_latest else None,
                "export_cost_usd_per_container": round(exp_latest, 2) if exp_latest else None,
                "import_cost_ratio_to_median": round(imp_ratio, 3),
                "export_cost_ratio_to_median": round(exp_ratio, 3),
                "combined_cost_ratio": round(combined_ratio, 3),
                "median_import_reference_usd": MEDIAN_IMPORT_COST_USD,
                "median_export_reference_usd": MEDIAN_EXPORT_COST_USD,
                "n_import_obs": len(imp_vals),
                "n_export_obs": len(exp_vals),
            },
        }
