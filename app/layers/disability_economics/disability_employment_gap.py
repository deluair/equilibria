"""Disability employment gap: gap between disabled and non-disabled employment rates.

Workers with disabilities face persistent barriers to labor market participation --
discrimination, inadequate accommodations, transport constraints, and employer
bias. The employment gap proxies this structural exclusion using vulnerable
employment share (SL.EMP.VULN.ZS) and part-time/low-quality work prevalence
(SL.TLF.PART.ZS) as indicators of labor market marginalization.

Score: low vulnerable + low part-time -> STABLE inclusive labor market.
High vulnerable + high part-time -> CRISIS exclusion with broad economic costs.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DisabilityEmploymentGap(LayerBase):
    layer_id = "lDI"
    name = "Disability Employment Gap"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        vuln_code = "SL.EMP.VULN.ZS"
        part_code = "SL.TLF.PART.ZS"

        vuln_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (vuln_code, "%vulnerable employment%"),
        )
        part_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (part_code, "%part-time%"),
        )

        vuln_vals = [r["value"] for r in vuln_rows if r["value"] is not None]
        part_vals = [r["value"] for r in part_rows if r["value"] is not None]

        if not vuln_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for SL.EMP.VULN.ZS"}

        vuln_latest = vuln_vals[0]
        part_latest = part_vals[0] if part_vals else None

        # Vulnerable employment is the primary signal (% of total employment)
        # Part-time adds a modifier if available
        base_score: float
        if vuln_latest < 10:
            base_score = 10.0 + vuln_latest * 1.0
        elif vuln_latest < 30:
            base_score = 20.0 + (vuln_latest - 10) * 1.25
        elif vuln_latest < 60:
            base_score = 45.0 + (vuln_latest - 30) * 0.833
        else:
            base_score = min(100.0, 70.0 + (vuln_latest - 60) * 0.75)

        # Part-time modifier: elevates score by up to 10 points
        if part_latest is not None:
            modifier = min(10.0, part_latest * 0.25)
            score = min(100.0, base_score + modifier)
        else:
            score = base_score

        vuln_trend = round(vuln_vals[0] - vuln_vals[-1], 3) if len(vuln_vals) > 1 else None

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "vulnerable_employment_pct": round(vuln_latest, 2),
                "part_time_employment_pct": round(part_latest, 2) if part_latest is not None else None,
                "vuln_trend": vuln_trend,
                "n_obs_vuln": len(vuln_vals),
                "n_obs_part": len(part_vals),
            },
        }
