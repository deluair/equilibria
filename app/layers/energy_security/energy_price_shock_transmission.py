"""Energy price shock transmission: pass-through of global energy prices to domestic inflation.

Countries heavily dependent on energy imports transmit global price spikes directly
into domestic CPI, amplifying inflation and reducing central bank policy space.
The pass-through is proxied by the joint signal of energy import dependence
(EG.IMP.CONS.ZS) and headline CPI inflation (FP.CPI.TOTL.ZG). High import
dependence combined with elevated inflation suggests active shock transmission.

Score: low dependence + low inflation -> STABLE, high dependence + high inflation
-> CRISIS active transmission with macro stress.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class EnergyPriceShockTransmission(LayerBase):
    layer_id = "lES"
    name = "Energy Price Shock Transmission"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        imp_code = "EG.IMP.CONS.ZS"
        cpi_code = "FP.CPI.TOTL.ZG"

        imp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (imp_code, "%Energy imports%"),
        )
        cpi_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (cpi_code, "%inflation%"),
        )

        imp_vals = [r["value"] for r in imp_rows if r["value"] is not None]
        cpi_vals = [r["value"] for r in cpi_rows if r["value"] is not None]

        if not imp_vals or not cpi_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data for energy imports or CPI (EG.IMP.CONS.ZS, FP.CPI.TOTL.ZG)",
            }

        import_dep = max(0.0, imp_vals[0])  # treat net exporters as 0 exposure
        cpi_inflation = cpi_vals[0]

        # Pass-through index: import dependence (0-100%) * |inflation| / 100
        # Clamped at positive inflation (negative deflation is different risk)
        inflation_stress = max(0.0, cpi_inflation)
        passthrough_index = import_dep * inflation_stress / 100.0

        # Score mapping: index thresholds
        if passthrough_index < 1.0:
            score = 5.0 + passthrough_index * 15.0
        elif passthrough_index < 5.0:
            score = 20.0 + (passthrough_index - 1.0) * 5.0
        elif passthrough_index < 15.0:
            score = 40.0 + (passthrough_index - 5.0) * 3.0
        else:
            score = min(100.0, 70.0 + (passthrough_index - 15.0) * 1.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "energy_import_dependence_pct": round(import_dep, 2),
                "cpi_inflation_pct": round(cpi_inflation, 2),
                "passthrough_index": round(passthrough_index, 4),
                "n_obs_imports": len(imp_vals),
                "n_obs_cpi": len(cpi_vals),
                "transmission_level": (
                    "low" if passthrough_index < 1.0
                    else "moderate" if passthrough_index < 5.0
                    else "high" if passthrough_index < 15.0
                    else "severe"
                ),
            },
        }
