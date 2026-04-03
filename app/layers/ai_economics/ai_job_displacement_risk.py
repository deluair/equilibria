"""AI job displacement risk: automation risk via routine task intensity proxies.

Automation risk is highest in economies with large manufacturing and low-skilled
service sectors where routine task intensity is elevated. ICT indicators (internet
users, secure servers) proxy digital infrastructure readiness that accelerates AI
adoption. High manufacturing share in low-ICT economies signals high displacement
risk because workers lack reskilling pathways.

Frey and Osborne (2017): 47% of US jobs at high automation risk. Risk is higher in
developing economies with large routine-task-intensive workforces.

Score: high manufacturing share + low ICT -> CRISIS displacement, low manufacturing
+ high ICT -> STABLE with managed transition.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AIJobDisplacementRisk(LayerBase):
    layer_id = "lAI"
    name = "AI Job Displacement Risk"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        manuf_code = "NV.IND.MANF.ZS"
        ict_code = "IT.NET.USER.ZS"

        manuf_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (manuf_code, "%manufacturing%value added%"),
        )
        ict_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ict_code, "%internet users%"),
        )

        manuf_vals = [r["value"] for r in manuf_rows if r["value"] is not None]
        ict_vals = [r["value"] for r in ict_rows if r["value"] is not None]

        if not manuf_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for manufacturing value added NV.IND.MANF.ZS",
            }

        manuf_share = manuf_vals[0]
        ict_penetration = ict_vals[0] if ict_vals else None

        # Base score from manufacturing share (routine task proxy)
        if manuf_share < 10:
            base = 15.0
        elif manuf_share < 20:
            base = 15.0 + (manuf_share - 10) * 2.5
        elif manuf_share < 30:
            base = 40.0 + (manuf_share - 20) * 2.0
        else:
            base = min(90.0, 60.0 + (manuf_share - 30) * 1.5)

        # ICT penetration modifies displacement risk
        # High ICT = better reskilling pathways, lower net risk
        if ict_penetration is not None:
            if ict_penetration >= 80:
                base = max(5.0, base - 20.0)
            elif ict_penetration >= 60:
                base = max(5.0, base - 12.0)
            elif ict_penetration >= 40:
                base = max(5.0, base - 5.0)
            elif ict_penetration < 20:
                base = min(100.0, base + 10.0)  # low digital access deepens risk

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "manufacturing_share_gdp_pct": round(manuf_share, 2),
                "internet_users_pct": round(ict_penetration, 2) if ict_penetration is not None else None,
                "n_obs_manufacturing": len(manuf_vals),
                "n_obs_ict": len(ict_vals),
                "high_displacement_risk": score > 50,
                "digital_reskilling_capacity": (
                    "high" if ict_penetration is not None and ict_penetration >= 60
                    else "medium" if ict_penetration is not None and ict_penetration >= 30
                    else "low"
                ),
            },
        }
