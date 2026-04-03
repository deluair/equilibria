"""Education learning loss: school enrollment gap and government education spending trajectory.

UNESCO estimated 1.6 billion students were affected by school closures in 2020.
The World Bank projects COVID-19 learning losses could reduce lifetime earnings by
$10 trillion. Two channels are tracked: (1) primary school enrollment (WDI
SE.PRM.ENRR) as a proxy for re-engagement, and (2) government education
expenditure as % of GDP (WDI SE.XPD.TOTL.GD.ZS) as a signal of remediation effort.

Persistent enrollment drops below pre-pandemic levels indicate children dropping
out permanently. Education spending stagnation prevents recovery programs.

Score: enrollment recovered + spending increased -> STABLE,
partial recovery -> WATCH, enrollment gap + stagnant spending -> STRESS,
large enrollment drop + spending cut -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class EducationLearningLoss(LayerBase):
    layer_id = "lPC"
    name = "Education Learning Loss"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        enroll_code = "SE.PRM.ENRR"
        spend_code = "SE.XPD.TOTL.GD.ZS"

        enroll_rows = await db.fetch_all(
            "SELECT value, date FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (enroll_code, "%primary school enrollment%"),
        )
        spend_rows = await db.fetch_all(
            "SELECT value, date FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (spend_code, "%government expenditure on education%"),
        )

        enroll_vals = [(r["date"], r["value"]) for r in enroll_rows if r["value"] is not None]
        spend_vals = [(r["date"], r["value"]) for r in spend_rows if r["value"] is not None]

        if not enroll_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SE.PRM.ENRR or SE.XPD.TOTL.GD.ZS",
            }

        enroll_vals.sort(key=lambda x: x[0])
        spend_vals.sort(key=lambda x: x[0])

        latest_enroll = enroll_vals[-1][1]
        pre_enroll = [v for d, v in enroll_vals if d < "2020-01-01"]
        baseline_enroll = pre_enroll[-1] if pre_enroll else enroll_vals[0][1]

        if baseline_enroll > 0:
            enroll_gap_pct = max(0.0, (baseline_enroll - latest_enroll) / baseline_enroll * 100)
        else:
            enroll_gap_pct = 0.0

        # Base score from enrollment gap
        if enroll_gap_pct < 1:
            base = 5.0
        elif enroll_gap_pct < 5:
            base = 5.0 + enroll_gap_pct * 5.0
        elif enroll_gap_pct < 15:
            base = 30.0 + (enroll_gap_pct - 5) * 3.0
        else:
            base = min(90.0, 60.0 + (enroll_gap_pct - 15) * 2.0)

        # Spending adjustment
        spend_change = None
        if spend_vals:
            latest_spend = spend_vals[-1][1]
            pre_spend = [v for d, v in spend_vals if d < "2020-01-01"]
            baseline_spend = pre_spend[-1] if pre_spend else spend_vals[0][1]
            if baseline_spend > 0:
                spend_change = (latest_spend - baseline_spend) / baseline_spend * 100
                # Spending increase mitigates score; spending cut worsens it
                if spend_change > 10:
                    base = max(5.0, base - 8.0)
                elif spend_change > 0:
                    base = max(5.0, base - 3.0)
                elif spend_change < -10:
                    base = min(100.0, base + 10.0)
                elif spend_change < 0:
                    base = min(100.0, base + 5.0)

        return {
            "score": round(base, 2),
            "signal": self.classify_signal(base),
            "metrics": {
                "latest_primary_enrollment_pct": round(latest_enroll, 2),
                "pre_pandemic_enrollment_pct": round(baseline_enroll, 2),
                "enrollment_gap_pct": round(enroll_gap_pct, 2),
                "education_spend_change_pct": round(spend_change, 2) if spend_change is not None else None,
                "n_enroll_obs": len(enroll_vals),
                "n_spend_obs": len(spend_vals),
            },
        }
