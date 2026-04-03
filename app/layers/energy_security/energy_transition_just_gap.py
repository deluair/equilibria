"""Energy transition just gap: fossil fuel employment exposure in transition-risk regions.

A just energy transition requires managing the social and economic displacement
of workers in fossil fuel sectors. Countries with a high share of employment
in mining, utilities, and fossil energy face larger adjustment costs.
Proxied via WDI SL.IND.EMPL.ZS (industry employment % of total) as a
structural indicator of fossil economy embeddedness and transition friction.

Score: low fossil sector employment share -> STABLE smooth transition,
moderate -> WATCH, high -> STRESS with significant adjustment burden,
very high -> CRISIS potential social instability from rapid transition.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class EnergyTransitionJustGap(LayerBase):
    layer_id = "lES"
    name = "Energy Transition Just Gap"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        ind_code = "SL.IND.EMPL.ZS"
        coal_code = "NY.GDP.COAL.RT.ZS"

        ind_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ind_code, "%industry employment%"),
        )
        coal_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (coal_code, "%coal rents%"),
        )

        ind_vals = [r["value"] for r in ind_rows if r["value"] is not None]
        coal_vals = [r["value"] for r in coal_rows if r["value"] is not None]

        if not ind_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for industry employment SL.IND.EMPL.ZS",
            }

        ind_share = ind_vals[0]
        coal_rent = coal_vals[0] if coal_vals else 0.0

        # Composite just-gap index: industry employment share weighted by coal rents
        # (higher coal rents = larger fossil embeddedness multiplier)
        coal_multiplier = 1.0 + min(coal_rent * 0.2, 1.0)
        just_gap_index = ind_share * coal_multiplier

        # Score: higher index = larger transition adjustment burden
        if just_gap_index < 15:
            score = 5.0 + just_gap_index * 1.0
        elif just_gap_index < 30:
            score = 20.0 + (just_gap_index - 15) * 1.67
        elif just_gap_index < 50:
            score = 45.0 + (just_gap_index - 30) * 1.25
        else:
            score = min(100.0, 70.0 + (just_gap_index - 50) * 0.6)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "industry_employment_share_pct": round(ind_share, 2),
                "coal_rents_gdp_pct": round(coal_rent, 3),
                "coal_multiplier": round(coal_multiplier, 3),
                "just_gap_index": round(just_gap_index, 3),
                "n_obs_industry": len(ind_vals),
                "n_obs_coal": len(coal_vals),
                "transition_burden": (
                    "low" if just_gap_index < 15
                    else "moderate" if just_gap_index < 30
                    else "high" if just_gap_index < 50
                    else "critical"
                ),
            },
        }
