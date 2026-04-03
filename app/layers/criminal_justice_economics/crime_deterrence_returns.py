"""Crime deterrence returns: elasticity of crime to policing and justice spending.

Becker (1968) established that crime responds to expected punishment probability and
severity. Empirical estimates suggest elasticity of crime to police presence ranges
from -0.3 to -0.5 -- a 10% increase in police reduces crime by 3-5%. Returns diminish
at high policing levels. This module proxies deterrence effectiveness by comparing
rule of law institutional quality against crime outcomes.

Score: strong rule of law with low crime (high deterrence returns) -> STABLE,
moderate institutions -> WATCH, weak rule of law + persistent crime -> STRESS,
failed deterrence with high crime despite spending -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CrimeDeterrenceReturns(LayerBase):
    layer_id = "lCJ"
    name = "Crime Deterrence Returns"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # WDI: Rule of Law estimate (-2.5 to 2.5, higher = better)
        rol_code = "RL.EST"
        rol_name = "rule of law"

        hom_code = "VC.IHR.PSRC.P5"
        hom_name = "homicide"

        rol_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rol_code, f"%{rol_name}%"),
        )
        hom_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (hom_code, f"%{hom_name}%"),
        )

        rol_vals = [r["value"] for r in rol_rows if r["value"] is not None]
        hom_vals = [r["value"] for r in hom_rows if r["value"] is not None]

        if not rol_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for rule of law estimate RL.EST",
            }

        rule_of_law = rol_vals[0]  # -2.5 to 2.5
        homicide = hom_vals[0] if hom_vals else None

        # Weak rule of law = low deterrence = higher score (stress)
        # Map: -2.5 -> 90, 2.5 -> 10
        base = round(((2.5 - rule_of_law) / 5.0) * 80.0 + 10.0, 2)
        base = max(0.0, min(100.0, base))

        # If rule of law is strong but crime remains high, deterrence returns are low
        # (diminishing returns / structural crime problem)
        if homicide is not None and rule_of_law > 0.5 and homicide > 10:
            base = min(100.0, base + 15.0)
        elif homicide is not None and homicide > 20:
            base = min(100.0, base + 10.0)
        elif homicide is not None and homicide < 2 and rule_of_law > 1.0:
            base = max(0.0, base - 5.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "rule_of_law_estimate": round(rule_of_law, 3),
                "homicide_rate_per_100k": round(homicide, 2) if homicide is not None else None,
                "n_obs_rol": len(rol_vals),
                "n_obs_crime": len(hom_vals),
                "deterrence_effectiveness": (
                    "high" if score < 25
                    else "moderate" if score < 50
                    else "low" if score < 75
                    else "failed"
                ),
            },
        }
