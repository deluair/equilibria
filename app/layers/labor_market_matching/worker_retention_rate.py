"""Worker retention rate: job tenure length and voluntary separation rate proxy.

High worker turnover imposes repeated matching costs on both firms and workers,
reducing allocative efficiency. Short average job tenure indicates poor initial
job matching quality -- workers or employers discover mismatches quickly and
separate. Voluntary separation rates distinguish workers seeking better matches
(healthy churn) from involuntary displacement (structural failure).

Score: long tenure / low separation -> STABLE, moderate turnover -> WATCH,
high separation rates -> STRESS poor initial match quality, extreme churn ->
CRISIS market unable to sustain productive employment relationships.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class WorkerRetentionRate(LayerBase):
    layer_id = "lLM"
    name = "Worker Retention Rate"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        vuln_code = "SL.EMP.VULN.ZS"
        self_emp_code = "SL.EMP.SELF.ZS"

        vuln_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (vuln_code, "%vulnerable employment%"),
        )
        self_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (self_emp_code, "%self.employed%"),
        )

        vuln_vals = [r["value"] for r in vuln_rows if r["value"] is not None]
        self_vals = [r["value"] for r in self_rows if r["value"] is not None]

        if not vuln_vals and not self_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for vulnerable employment SL.EMP.VULN.ZS or self-employment SL.EMP.SELF.ZS",
            }

        # Vulnerable employment share = workers in precarious, low-tenure arrangements
        if vuln_vals:
            precarity = vuln_vals[0]
            trend = round(vuln_vals[0] - vuln_vals[-1], 3) if len(vuln_vals) > 1 else None
        else:
            precarity = self_vals[0]
            trend = round(self_vals[0] - self_vals[-1], 3) if len(self_vals) > 1 else None

        # High vulnerable/self-employment share -> low retention -> higher score
        if precarity < 10:
            score = precarity * 1.5
        elif precarity < 25:
            score = 15.0 + (precarity - 10) * 1.5
        elif precarity < 50:
            score = 37.5 + (precarity - 25) * 1.2
        else:
            score = min(100.0, 67.5 + (precarity - 50) * 0.65)

        # If self-employment is also high alongside vulnerability, amplify
        if vuln_vals and self_vals:
            self_share = self_vals[0]
            if self_share > 40:
                score = min(100.0, score + 5.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "vulnerable_employment_pct": round(vuln_vals[0], 2) if vuln_vals else None,
                "self_employment_pct": round(self_vals[0], 2) if self_vals else None,
                "precarity_index": round(precarity, 2),
                "precarity_trend": trend,
                "n_obs_vuln": len(vuln_vals),
                "n_obs_self": len(self_vals),
                "retention_quality": "good" if score < 25 else "poor" if score > 50 else "moderate",
            },
        }
