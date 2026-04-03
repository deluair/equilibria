"""Bureaucratic Efficiency module.

Combines two dimensions:
1. IC.REG.DURS: Days to start a business (World Bank Doing Business).
   Long startup times signal bureaucratic red tape, entry barriers, and rent-seeking.
2. GE.EST: Government Effectiveness (WGI). Captures quality of public services,
   civil service independence, policy credibility, and implementation capacity.

Both are mapped to stress (0-1) and averaged. High combined stress = inefficient
bureaucracy that raises firm costs and discourages formal sector participation.

References:
    World Bank. (2023). Doing Business / Business Ready Indicators.
    World Bank. (2023). Worldwide Governance Indicators (GE.EST).
    Djankov, S. et al. (2002). The Regulation of Entry. QJE 117(1), 1-37.
    Evans, P. & Rauch, J. (1999). Bureaucracy and Growth. ASR 64(5), 748-765.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class BureaucraticEfficiency(LayerBase):
    layer_id = "lIE"
    name = "Bureaucratic Efficiency"

    async def compute(self, db, **kwargs) -> dict:
        reg_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("IC.REG.DURS", "%days to start%business%"),
        )
        ge_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("GE.EST", "%government effectiveness%"),
        )

        if not reg_rows and not ge_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no bureaucratic efficiency data"}

        metrics = {}
        stresses = []

        if reg_rows:
            days = float(reg_rows[0]["value"])
            # 0-5d = minimal, 5-20 moderate, 20-60 high, >60 severe
            if days <= 5:
                s = days / 5.0 * 0.15
            elif days <= 20:
                s = 0.15 + (days - 5) / 15.0 * 0.30
            elif days <= 60:
                s = 0.45 + (days - 20) / 40.0 * 0.35
            else:
                s = 0.80 + min((days - 60) / 60.0 * 0.20, 0.20)
            s = max(0.0, min(1.0, s))
            stresses.append(s)
            metrics["startup_days"] = round(days, 1)
            metrics["startup_stress"] = round(s, 4)

        if ge_rows:
            ge = float(ge_rows[0]["value"])
            # WGI GE.EST: -2.5 (worst) to 2.5 (best), invert to stress
            ge_stress = 1.0 - (ge + 2.5) / 5.0
            ge_stress = max(0.0, min(1.0, ge_stress))
            stresses.append(ge_stress)
            metrics["ge_est"] = round(ge, 4)
            metrics["ge_stress"] = round(ge_stress, 4)

        composite_stress = sum(stresses) / len(stresses)
        score = round(composite_stress * 100.0, 2)
        metrics["n_indicators"] = len(stresses)

        return {
            "score": score,
            "metrics": metrics,
            "reference": "WB IC.REG.DURS + GE.EST; Djankov et al. 2002; Evans & Rauch 1999",
        }
