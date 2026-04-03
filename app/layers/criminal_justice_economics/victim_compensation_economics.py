"""Victim compensation economics: social cost of crime including victim productivity loss.

Crime imposes costs beyond direct losses -- victims suffer reduced labor force participation,
psychological trauma, healthcare utilization, and productivity decline. The RAND Corporation
estimates the total social cost of a violent crime at 3-10x the direct property loss.
Social protection expenditure and injury/mortality rates proxy the aggregate victim burden.

Score: low injury mortality + adequate social protection -> STABLE,
moderate burden -> WATCH, high violent injury burden -> STRESS,
severe victim economic exclusion -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class VictimCompensationEconomics(LayerBase):
    layer_id = "lCJ"
    name = "Victim Compensation Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # WDI: Cause of death by injury (% of total deaths)
        inj_code = "SH.DTH.INJR.ZS"
        inj_name = "death by injury"

        # Social protection expenditure % GDP as compensation capacity proxy
        sp_code = "per_allsp.cov_pop_tot"
        sp_name = "social protection coverage"

        inj_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (inj_code, f"%{inj_name}%"),
        )
        sp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (sp_code, f"%{sp_name}%"),
        )

        inj_vals = [r["value"] for r in inj_rows if r["value"] is not None]
        sp_vals = [r["value"] for r in sp_rows if r["value"] is not None]

        if not inj_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for victim burden proxy SH.DTH.INJR.ZS",
            }

        injury_pct = inj_vals[0]
        sp_coverage = sp_vals[0] if sp_vals else None
        trend = round(inj_vals[0] - inj_vals[-1], 3) if len(inj_vals) > 1 else None

        # Score from injury mortality share
        if injury_pct < 3:
            base = 8.0 + injury_pct * 3.0
        elif injury_pct < 8:
            base = 17.0 + (injury_pct - 3) * 5.0
        elif injury_pct < 15:
            base = 42.0 + (injury_pct - 8) * 3.0
        else:
            score_val = min(100.0, 63.0 + (injury_pct - 15) * 2.0)
            base = score_val

        # Low social protection coverage amplifies victim economic harm
        if sp_coverage is not None and sp_coverage < 30:
            base = min(100.0, base + 10.0)
        elif sp_coverage is not None and sp_coverage < 60:
            base = min(100.0, base + 5.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "injury_death_pct": round(injury_pct, 2),
                "social_protection_coverage_pct": round(sp_coverage, 2) if sp_coverage is not None else None,
                "trend": trend,
                "n_obs_injury": len(inj_vals),
                "n_obs_sp": len(sp_vals),
                "victim_burden": (
                    "low" if score < 25
                    else "moderate" if score < 50
                    else "high" if score < 75
                    else "severe"
                ),
            },
        }
