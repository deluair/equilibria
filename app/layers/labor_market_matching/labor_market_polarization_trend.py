"""Labor market polarization trend: middle-skill job share decline vs high/low skill growth.

Polarization describes the simultaneous growth of high-skill (managerial, professional)
and low-skill (service) employment while middle-skill (routine, clerical, production)
jobs are automated away. This hollowing-out of the wage distribution creates matching
challenges as displaced middle-skill workers lack credentials for high-skill roles
and face competition in low-skill service jobs.

Score: balanced skill distribution -> STABLE, moderate hollowing -> WATCH,
strong polarization -> STRESS with significant displacement, severe polarization ->
CRISIS with large structural unemployment among middle-skill workers.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class LaborMarketPolarizationTrend(LayerBase):
    layer_id = "lLM"
    name = "Labor Market Polarization Trend"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        high_skill_code = "SL.EMP.SMGT.ZS"
        service_code = "SL.SRV.EMPL.ZS"
        industry_code = "SL.IND.EMPL.ZS"

        high_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (high_skill_code, "%skilled%employment%"),
        )
        service_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (service_code, "%employment.*service%"),
        )
        industry_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (industry_code, "%employment.*industry%"),
        )

        high_vals = [r["value"] for r in high_rows if r["value"] is not None]
        service_vals = [r["value"] for r in service_rows if r["value"] is not None]
        industry_vals = [r["value"] for r in industry_rows if r["value"] is not None]

        if not service_vals and not industry_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for employment by sector (services/industry)",
            }

        # Polarization proxy: service employment share growth + declining industry share
        service_share = service_vals[0] if service_vals else None
        industry_share = industry_vals[0] if industry_vals else None

        service_trend = (
            round(service_vals[0] - service_vals[-1], 3) if len(service_vals) > 1 else None
        )
        industry_trend = (
            round(industry_vals[0] - industry_vals[-1], 3) if len(industry_vals) > 1 else None
        )

        # Polarization index: high services + declining industry = structural shift
        # Industry share acts as proxy for middle-skill manufacturing/routine jobs
        if industry_share is not None:
            # Declining industry share signals displacement of middle-skill workers
            base_score = max(0.0, 50.0 - industry_share)  # lower industry = more polarized
            if industry_trend is not None and industry_trend < -2.0:
                base_score = min(100.0, base_score + 15.0)
            elif industry_trend is not None and industry_trend < -0.5:
                base_score = min(100.0, base_score + 5.0)
        elif service_share is not None:
            # Very high service share with low skilled employment = polarized toward low end
            base_score = max(0.0, service_share - 50.0) if service_share > 50 else 10.0
        else:
            base_score = 30.0

        # High skill share moderates polarization impact if growing
        if high_vals:
            high_share = high_vals[0]
            if high_share > 30:
                base_score = max(0.0, base_score - 8.0)  # high-skill growth partially offsets

        score = round(base_score, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "service_employment_share_pct": round(service_share, 2) if service_share is not None else None,
                "industry_employment_share_pct": round(industry_share, 2) if industry_share is not None else None,
                "high_skill_employment_share_pct": round(high_vals[0], 2) if high_vals else None,
                "service_trend_pct": service_trend,
                "industry_trend_pct": industry_trend,
                "n_obs_service": len(service_vals),
                "n_obs_industry": len(industry_vals),
                "polarization_risk": "high" if score > 50 else "moderate" if score > 25 else "low",
            },
        }
