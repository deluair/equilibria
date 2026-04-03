"""Skill mismatch rate: education-occupation mismatch via tertiary enrollment vs skilled employment.

Skill mismatch occurs when workers' qualifications diverge from job requirements.
High tertiary enrollment relative to skilled employment share signals over-education
or qualification inflation -- graduates cannot find jobs matching their credentials.
Conversely, low enrollment with high skilled-job demand signals under-supply of
qualified workers.

Score: balanced enrollment-employment ratio -> STABLE, moderate gap -> WATCH,
large over-supply or under-supply -> STRESS/CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SkillMismatchRate(LayerBase):
    layer_id = "lLM"
    name = "Skill Mismatch Rate"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        enroll_code = "SE.TER.ENRR"
        skilled_code = "SL.EMP.SMGT.ZS"

        enroll_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (enroll_code, "%tertiary%enrollment%"),
        )
        skilled_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (skilled_code, "%skilled%employment%"),
        )

        enroll_vals = [r["value"] for r in enroll_rows if r["value"] is not None]

        if not enroll_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for tertiary enrollment SE.TER.ENRR",
            }

        latest_enroll = enroll_vals[0]
        skilled_vals = [r["value"] for r in skilled_rows if r["value"] is not None]

        if skilled_vals:
            latest_skilled = skilled_vals[0]
            # Mismatch gap: difference between tertiary enrollment rate and skilled employment share
            gap = abs(latest_enroll - latest_skilled)
            mismatch_type = "over_educated" if latest_enroll > latest_skilled else "under_supplied"
        else:
            # Without skilled employment data, use enrollment level as proxy
            # Very high enrollment without matching absorptive capacity signals mismatch
            gap = max(0.0, latest_enroll - 40.0)
            mismatch_type = "unknown"

        enroll_trend = round(enroll_vals[0] - enroll_vals[-1], 3) if len(enroll_vals) > 1 else None

        if gap < 10:
            score = gap * 2.0
        elif gap < 25:
            score = 20.0 + (gap - 10) * 2.0
        elif gap < 50:
            score = 50.0 + (gap - 25) * 1.0
        else:
            score = min(100.0, 75.0 + (gap - 50) * 0.5)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "tertiary_enrollment_rate": round(latest_enroll, 2),
                "skilled_employment_share": round(skilled_vals[0], 2) if skilled_vals else None,
                "mismatch_gap_pct": round(gap, 2),
                "mismatch_type": mismatch_type,
                "enrollment_trend": enroll_trend,
                "n_obs_enrollment": len(enroll_vals),
            },
        }
