"""Eldercare infrastructure gap: elderly population vs beds and workforce supply.

As populations age, demand for long-term care, geriatric wards, nursing
homes, and specialized health workers grows substantially. Hospital bed
availability (SH.MED.BEDS.ZS, beds per 1,000 people) serves as a proxy
for physical care infrastructure. The gap between elderly population share
and available beds/workforce signals unmet eldercare demand.

High elderly share with low bed availability = infrastructure gap = STRESS.
Low elderly share with adequate beds = capacity surplus = STABLE.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ElderCareInfrastructureGap(LayerBase):
    layer_id = "lAG"
    name = "Eldercare Infrastructure Gap"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        pop_code = "SP.POP.65UP.TO.ZS"
        beds_code = "SH.MED.BEDS.ZS"

        pop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pop_code, "%Population ages 65%"),
        )
        beds_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (beds_code, "%hospital beds%"),
        )

        pop_vals = [r["value"] for r in pop_rows if r["value"] is not None]
        beds_vals = [r["value"] for r in beds_rows if r["value"] is not None]

        if not pop_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for elderly population share SP.POP.65UP.TO.ZS",
            }

        elderly_share = pop_vals[0]
        beds_per_1000 = beds_vals[0] if beds_vals else None

        # Expected beds needed scales with elderly share
        # Rule of thumb: 3 beds per 1,000 total population for general care,
        # elderly-intensive countries need ~5-10 beds per 1,000
        expected_beds = 2.0 + elderly_share * 0.3  # rough scaling

        if beds_per_1000 is not None:
            gap_ratio = max(0.0, expected_beds - beds_per_1000) / expected_beds
        else:
            # No data -> assume moderate gap based on elderly share alone
            gap_ratio = min(1.0, elderly_share / 30.0)

        # Score: larger gap -> higher stress
        score = round(min(100.0, gap_ratio * 100.0 * 1.1), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "elderly_share_pct": round(elderly_share, 2),
                "beds_per_1000": round(beds_per_1000, 2) if beds_per_1000 is not None else None,
                "expected_beds_per_1000": round(expected_beds, 2),
                "infrastructure_gap_ratio": round(gap_ratio, 4),
                "n_obs_pop": len(pop_vals),
                "n_obs_beds": len(beds_vals),
                "gap_critical": gap_ratio > 0.4,
            },
        }
