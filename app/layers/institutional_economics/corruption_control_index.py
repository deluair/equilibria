"""Corruption Control Index module.

Uses World Bank WGI CC.EST (Control of Corruption estimate).
High corruption raises transaction costs, distorts resource allocation, reduces
investment, and erodes institutional trust. The WGI CC.EST is inverted here:
a lower (more negative) value means more corruption and maps to higher stress.

WGI range: -2.5 (worst) to 2.5 (best governance). Rescaled to 0-100 stress.

References:
    Kaufmann, D., Kraay, A. & Mastruzzi, M. (2010). WGI 1996-2009. World Bank.
    Mauro, P. (1995). Corruption and Growth. QJE 110(3), 681-712.
    Ades, A. & Di Tella, R. (1999). Rents, Competition, and Corruption. AER 89(4).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CorruptionControlIndex(LayerBase):
    layer_id = "lIE"
    name = "Corruption Control Index"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("CC.EST", "%control of corruption%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no corruption control data"}

        val = float(rows[0]["value"])

        # WGI CC.EST: -2.5 (high corruption) to 2.5 (low corruption)
        # Stress = inverted: high corruption -> high stress
        stress = 1.0 - (val + 2.5) / 5.0
        stress = max(0.0, min(1.0, stress))
        score = round(stress * 100.0, 2)

        corruption_level = (
            "low" if val > 1.0
            else "moderate_low" if val > 0.0
            else "moderate_high" if val > -1.0
            else "high"
        )

        return {
            "score": score,
            "metrics": {
                "cc_est": round(val, 4),
                "stress": round(stress, 4),
                "corruption_level": corruption_level,
                "n_obs": len(rows),
                "scale": "WGI -2.5 (worst) to 2.5 (best)",
            },
            "reference": "WB CC.EST; Mauro 1995 QJE; Kaufmann et al. 2010",
        }
