"""Rule of Law Score module.

Uses World Bank WGI RL.EST (Rule of Law estimate), inverted to a stress scale.
Rule of law captures the extent to which agents have confidence in and abide by
rules: contract enforcement quality, property rights, police and courts, and
likelihood of crime and violence.

Weak rule of law is a primary driver of institutional stress (North 1990,
Acemoglu et al. 2001). Rescaled from WGI -2.5/2.5 to 0-100 stress score.

References:
    World Bank. (2023). Worldwide Governance Indicators.
    Acemoglu, D., Johnson, S. & Robinson, J.A. (2001). Colonial Origins. AER 91(5).
    North, D.C. (1990). Institutions, Institutional Change and Economic Performance.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class RuleOfLawScore(LayerBase):
    layer_id = "lIE"
    name = "Rule of Law Score"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("RL.EST", "%rule of law%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no rule of law data"}

        vals = [float(r["value"]) for r in rows]
        latest = vals[0]

        # Invert WGI RL.EST (-2.5 to 2.5) into stress (0-1)
        stress = 1.0 - (latest + 2.5) / 5.0
        stress = max(0.0, min(1.0, stress))
        score = round(stress * 100.0, 2)

        # Trend from most recent to oldest (rows are DESC)
        trend_dir = None
        if len(vals) >= 3:
            # Compare first (most recent) to last (oldest)
            trend_dir = "improving" if vals[0] > vals[-1] else "deteriorating"

        tier = (
            "strong" if latest > 1.0
            else "moderate" if latest > 0.0
            else "weak" if latest > -1.0
            else "very_weak"
        )

        result = {
            "score": score,
            "metrics": {
                "rl_est_latest": round(latest, 4),
                "stress": round(stress, 4),
                "tier": tier,
                "n_obs": len(vals),
            },
            "reference": "WB RL.EST; Acemoglu et al. 2001 AER; North 1990",
        }
        if trend_dir:
            result["metrics"]["trend"] = trend_dir
        return result
