"""Dementia economic burden: elderly share x health cost proxy.

Dementia is among the most cost-intensive conditions in aging populations,
encompassing direct medical care, long-term care, and informal caregiver
costs. WHO estimates global dementia costs exceed $1.3 trillion annually.
The economic burden scales nonlinearly with elderly population share and
health system expenditure capacity.

This module proxies dementia burden via a composite of elderly population
share and health expenditure as a share of GDP, capturing the latent cost
pressure from age-related cognitive decline at the system level.

Score: high elderly share + high health expenditure -> STRESS/CRISIS,
young population + low health spend -> STABLE.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DementiaEconomicBurden(LayerBase):
    layer_id = "lAG"
    name = "Dementia Economic Burden"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        pop_code = "SP.POP.65UP.TO.ZS"
        he_code = "SH.XPD.CHEX.GD.ZS"

        pop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pop_code, "%Population ages 65%"),
        )
        he_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (he_code, "%health expenditure%"),
        )

        pop_vals = [r["value"] for r in pop_rows if r["value"] is not None]
        he_vals = [r["value"] for r in he_rows if r["value"] is not None]

        if not pop_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for elderly population share SP.POP.65UP.TO.ZS",
            }

        elderly_share = pop_vals[0]
        health_exp = he_vals[0] if he_vals else None

        # Dementia prevalence scales with elderly share (prevalence ~5% at 65, ~40% at 85+)
        # Approximate weight of dementia-related spend: ~10-15% of total health cost
        # Proxy burden: elderly_share * health_exp * 0.12 (rough dementia cost fraction)
        if health_exp is not None:
            dementia_burden_proxy = elderly_share * health_exp * 0.12 / 100.0
        else:
            dementia_burden_proxy = elderly_share * 0.05  # minimal proxy

        # Score: higher burden proxy -> higher stress
        if dementia_burden_proxy < 0.05:
            score = 10.0 + dementia_burden_proxy * 200.0
        elif dementia_burden_proxy < 0.15:
            score = 20.0 + (dementia_burden_proxy - 0.05) * 300.0
        elif dementia_burden_proxy < 0.30:
            score = 50.0 + (dementia_burden_proxy - 0.15) * 200.0
        else:
            score = min(100.0, 80.0 + (dementia_burden_proxy - 0.30) * 100.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "elderly_share_pct": round(elderly_share, 2),
                "health_expenditure_gdp_pct": round(health_exp, 2) if health_exp is not None else None,
                "dementia_burden_proxy": round(dementia_burden_proxy, 4),
                "n_obs_pop": len(pop_vals),
                "n_obs_health": len(he_vals),
            },
        }
