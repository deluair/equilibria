"""AI compute concentration: semiconductor import concentration and compute access inequality.

AI computation requires specialized hardware (GPUs, TPUs, ASICs) primarily
produced by a handful of firms in a few countries. Economies that rely heavily
on semiconductor imports with concentrated supplier bases face strategic
vulnerability: access to AI compute can be cut off by export controls, supply
chain disruptions, or geopolitical pressure. High-tech import dependency combined
with low domestic manufacturing signals compute access inequality.

US CHIPS Act (2022) and EU Chips Act (2023) reflect recognition that compute
concentration is a strategic risk. Economies outside the semiconductor value
chain are structurally disadvantaged in AI capacity building.

Score: high tech import dependency + no domestic semiconductor base -> CRISIS,
domestic semiconductor capacity + diversified supply -> STABLE.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AIComputeConcentration(LayerBase):
    layer_id = "lAI"
    name = "AI Compute Concentration"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        hitech_imp_code = "TM.VAL.ICTG.ZS.UN"
        hitech_exp_code = "TX.VAL.TECH.MF.ZS"

        imp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (hitech_imp_code, "%ICT goods imports%"),
        )
        exp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (hitech_exp_code, "%high-technology exports%"),
        )

        imp_vals = [r["value"] for r in imp_rows if r["value"] is not None]
        exp_vals = [r["value"] for r in exp_rows if r["value"] is not None]

        if not imp_vals and not exp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for ICT goods imports TM.VAL.ICTG.ZS.UN or high-tech exports TX.VAL.TECH.MF.ZS",
            }

        ict_imports = imp_vals[0] if imp_vals else None
        hitech_exports = exp_vals[0] if exp_vals else None

        # Base score: high ICT import dependency signals compute access vulnerability
        if ict_imports is not None:
            if ict_imports < 5:
                base = 20.0
            elif ict_imports < 15:
                base = 20.0 + (ict_imports - 5) * 2.0
            elif ict_imports < 25:
                base = 40.0 + (ict_imports - 15) * 1.5
            else:
                base = min(80.0, 55.0 + (ict_imports - 25) * 1.0)
        else:
            base = 50.0  # missing import data defaults to moderate concern

        # High-tech exports indicate domestic capacity -- reduces vulnerability
        if hitech_exports is not None:
            if hitech_exports >= 20:
                base = max(5.0, base - 25.0)
            elif hitech_exports >= 10:
                base = max(5.0, base - 15.0)
            elif hitech_exports >= 5:
                base = max(5.0, base - 8.0)
            elif hitech_exports < 1:
                base = min(100.0, base + 10.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "ict_imports_pct_total_imports": round(ict_imports, 2) if ict_imports is not None else None,
                "hitech_exports_pct_manufactured": round(hitech_exports, 2) if hitech_exports is not None else None,
                "n_obs_imports": len(imp_vals),
                "n_obs_exports": len(exp_vals),
                "compute_access_vulnerable": score > 50,
                "domestic_tech_capacity": hitech_exports is not None and hitech_exports >= 10,
            },
        }
