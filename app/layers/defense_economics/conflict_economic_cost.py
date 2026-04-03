"""Conflict economic cost: economic cost of conflict via WDI political risk indicators.

Active conflict and its aftermath impose enormous economic costs: capital
destruction, displacement, reduced foreign investment, and diversion of public
resources. This module uses the WGI Political Stability indicator (PV.EST) as
the primary conflict risk proxy, supplemented by military expenditure as % GDP
(MS.MIL.XPND.GD.ZS) which rises sharply in conflict and post-conflict states.

IEP's Global Peace Index estimates the global economic impact of violence at
~11-13% of global GDP annually (2023 data).

Score: stable + low military burden -> STABLE, deteriorating stability
+ rising defense spend -> STRESS/CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ConflictEconomicCost(LayerBase):
    layer_id = "lDX"
    name = "Conflict Economic Cost"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        pv_code = "PV.EST"
        mil_code = "MS.MIL.XPND.GD.ZS"

        pv_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (pv_code, "%political stability%violence%"),
        )
        mil_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (mil_code, "%military expenditure%GDP%"),
        )

        pv_vals = [r["value"] for r in pv_rows if r["value"] is not None]
        mil_vals = [r["value"] for r in mil_rows if r["value"] is not None]

        if not pv_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for political stability PV.EST",
            }

        pv = pv_vals[0]
        mil = mil_vals[0] if mil_vals else 2.0

        # WGI PV: -3 (conflict) to +3 (stable). Conflict cost rises steeply below -1.
        if pv >= 1.0:
            base = 5.0
        elif pv >= 0.0:
            base = 5.0 + (1.0 - pv) * 20.0
        elif pv >= -1.0:
            base = 25.0 + (-pv) * 25.0
        elif pv >= -2.0:
            base = 50.0 + (-pv - 1.0) * 20.0
        else:
            base = min(95.0, 70.0 + (-pv - 2.0) * 15.0)

        # High military spending as % GDP amplifies conflict cost signal
        mil_adj = max(0.0, (mil - 2.0) * 2.5)
        score = min(100.0, base + mil_adj)

        # Trend in stability (improvement = positive outlook)
        pv_trend = None
        if len(pv_vals) > 1:
            pv_trend = round(pv_vals[0] - pv_vals[-1], 3)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "political_stability_wgi": round(pv, 3),
                "military_spending_gdp_pct": round(mil, 3),
                "stability_trend": pv_trend,
                "n_obs_pv": len(pv_vals),
                "n_obs_mil": len(mil_vals),
                "conflict_risk": (
                    "minimal" if pv >= 1.0
                    else "low" if pv >= 0.0
                    else "elevated" if pv >= -1.0
                    else "high" if pv >= -2.0
                    else "conflict-affected"
                ),
            },
        }
