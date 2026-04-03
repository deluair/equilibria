"""Strategic petroleum reserves: oil import coverage from strategic stockpiles.

Strategic petroleum reserves (SPR) provide a buffer against supply disruptions.
The IEA standard is 90 days of net import coverage. Countries below this threshold
face acute vulnerability to short-term supply shocks. Proxied via oil import value
(BM.GSR.ENRG.ZS energy imports % of merchandise imports) combined with current
account reserves data (FI.RES.TOTL.MO) as months of import coverage.

Score: >90 days coverage -> STABLE, 60-90 days -> WATCH, 30-60 -> STRESS, <30 -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class StrategicPetroleumReserves(LayerBase):
    layer_id = "lES"
    name = "Strategic Petroleum Reserves"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        res_code = "FI.RES.TOTL.MO"
        enrg_code = "BM.GSR.ENRG.ZS"

        res_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (res_code, "%months of import%"),
        )
        enrg_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (enrg_code, "%energy imports%merchandise%"),
        )

        res_vals = [r["value"] for r in res_rows if r["value"] is not None]
        enrg_vals = [r["value"] for r in enrg_rows if r["value"] is not None]

        if not res_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for total reserves months of imports FI.RES.TOTL.MO",
            }

        total_months = res_vals[0]
        energy_share = enrg_vals[0] if enrg_vals else 15.0  # approximate if missing

        # Approximate energy-specific coverage = total months * energy_share / 100
        energy_months = total_months * energy_share / 100.0
        # Convert to days
        energy_days = energy_months * 30.0

        # Score: lower coverage = higher risk (IEA 90-day standard)
        if energy_days >= 90:
            score = max(0.0, 20.0 - (energy_days - 90) * 0.1)
        elif energy_days >= 60:
            score = 20.0 + (90.0 - energy_days) * 0.83
        elif energy_days >= 30:
            score = 45.0 + (60.0 - energy_days) * 0.83
        else:
            score = min(100.0, 70.0 + (30.0 - energy_days) * 1.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "total_reserves_months_imports": round(total_months, 2),
                "energy_imports_pct_merchandise": round(energy_share, 2),
                "estimated_energy_coverage_days": round(energy_days, 1),
                "n_obs_reserves": len(res_vals),
                "n_obs_energy_share": len(enrg_vals),
                "iea_standard_met": energy_days >= 90,
                "coverage_tier": (
                    "adequate" if energy_days >= 90
                    else "watch" if energy_days >= 60
                    else "stress" if energy_days >= 30
                    else "critical"
                ),
            },
        }
