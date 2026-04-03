"""Tourism recovery trajectory: international tourism receipts recovery.

International tourism collapsed ~74% in 2020 (UNWTO), the worst decline since
records began. Recovery has been uneven: some destinations returned to 2019 levels
by 2023, others remain significantly below. WDI ST.INT.RCPT.CD (international
tourism receipts, current USD) tracks the revenue dimension of the recovery.

Tourism represents >10% of GDP in many small states and a significant source of
foreign exchange earnings. Persistent shortfalls have cascading effects on
hospitality employment, current accounts, and local SME viability.

Score: full recovery or above 2019 level -> STABLE, 80-100% of 2019 -> WATCH,
50-80% of 2019 -> STRESS, below 50% of 2019 -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class TourismRecoveryTrajectory(LayerBase):
    layer_id = "lPC"
    name = "Tourism Recovery Trajectory"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "ST.INT.RCPT.CD"
        name = "international tourism receipts"
        rows = await db.fetch_all(
            "SELECT value, date FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for ST.INT.RCPT.CD",
            }

        values = [(r["date"], r["value"]) for r in rows if r["value"] is not None]
        if not values:
            return {"score": None, "signal": "UNAVAILABLE", "error": "all null values"}

        values.sort(key=lambda x: x[0])
        latest = values[-1][1]

        pre_covid = [v for d, v in values if d < "2020-01-01"]
        baseline_2019 = pre_covid[-1] if pre_covid else values[0][1]

        if baseline_2019 <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "zero or missing 2019 baseline"}

        recovery_ratio = latest / baseline_2019

        # Score: lower recovery ratio = higher stress
        if recovery_ratio >= 1.0:
            score = 5.0
        elif recovery_ratio >= 0.80:
            score = 5.0 + (1.0 - recovery_ratio) * 100.0
        elif recovery_ratio >= 0.50:
            score = 25.0 + (0.80 - recovery_ratio) * 83.3
        else:
            score = min(100.0, 50.0 + (0.50 - recovery_ratio) * 100.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "latest_receipts_usd": round(latest, 0),
                "pre_pandemic_baseline_usd": round(baseline_2019, 0),
                "recovery_ratio": round(recovery_ratio, 3),
                "recovery_pct_of_2019": round(recovery_ratio * 100, 1),
                "full_recovery_achieved": recovery_ratio >= 1.0,
                "n_obs": len(values),
            },
        }
