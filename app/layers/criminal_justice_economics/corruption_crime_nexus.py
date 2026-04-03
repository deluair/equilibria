"""Corruption-crime nexus: interaction between corruption and crime affecting investment.

Corruption and crime reinforce each other: corrupt officials facilitate organized crime,
while criminal organizations capture state institutions. Their interaction suppresses
foreign direct investment, distorts resource allocation, and raises the cost of doing
business. The World Bank Control of Corruption and Transparency International CPI
are standard proxies. Low scores indicate high corruption risk.

Score: strong control of corruption (high CPI) + low crime -> STABLE,
moderate corruption -> WATCH, weak institutions + high crime -> STRESS,
state capture + pervasive crime -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CorruptionCrimeNexus(LayerBase):
    layer_id = "lCJ"
    name = "Corruption Crime Nexus"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # WDI: Control of Corruption (estimate, -2.5 to 2.5, higher = better)
        cor_code = "CC.EST"
        cor_name = "control of corruption"

        hom_code = "VC.IHR.PSRC.P5"
        hom_name = "homicide"

        cor_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (cor_code, f"%{cor_name}%"),
        )
        hom_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (hom_code, f"%{hom_name}%"),
        )

        cor_vals = [r["value"] for r in cor_rows if r["value"] is not None]
        hom_vals = [r["value"] for r in hom_rows if r["value"] is not None]

        if not cor_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for corruption control estimate CC.EST",
            }

        corruption = cor_vals[0]  # range -2.5 to 2.5
        homicide = hom_vals[0] if hom_vals else None

        # Map corruption estimate to 0-100 scale: -2.5 -> 95, 2.5 -> 5
        # (worse corruption = higher score = more stress)
        base = round(((2.5 - corruption) / 5.0) * 90.0 + 5.0, 2)
        base = max(0.0, min(100.0, base))

        # Crime amplifier: high homicide rate compounds corruption impact
        if homicide is not None and homicide > 20:
            base = min(100.0, base + 12.0)
        elif homicide is not None and homicide > 8:
            base = min(100.0, base + 6.0)
        elif homicide is not None and homicide > 3:
            base = min(100.0, base + 3.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "corruption_control_estimate": round(corruption, 3),
                "homicide_rate_per_100k": round(homicide, 2) if homicide is not None else None,
                "n_obs_corruption": len(cor_vals),
                "n_obs_crime": len(hom_vals),
                "nexus_risk": (
                    "low" if score < 25
                    else "moderate" if score < 50
                    else "high" if score < 75
                    else "severe"
                ),
            },
        }
