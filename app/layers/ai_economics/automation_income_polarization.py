"""Automation income polarization: Gini trend in high-automation-risk economies.

Automation and AI disproportionately displace middle-skill routine workers while
complementing high-skill cognitive workers and (to a lesser extent) low-skill
manual service workers. This hollowing-out of middle-wage jobs -- labor market
polarization -- drives increases in income inequality as measured by the Gini
coefficient. The trend in the Gini, not just its level, captures whether
automation-driven polarization is currently worsening.

Acemoglu and Restrepo (2018): robots reduce employment and wages broadly, with
stronger negative effects in more automatable industries. Autor (2015) documents
U.S. labor market polarization since 1980.

Score: rising Gini trend -> CRISIS (polarization accelerating), stable low Gini
-> STABLE (redistribution mechanisms containing automation shocks).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AutomationIncomePolarization(LayerBase):
    layer_id = "lAI"
    name = "Automation Income Polarization"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gini_code = "SI.POV.GINI"
        vuln_code = "SL.EMP.VULN.ZS"

        gini_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gini_code, "%Gini%"),
        )
        vuln_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (vuln_code, "%vulnerable employment%"),
        )

        gini_vals = [r["value"] for r in gini_rows if r["value"] is not None]
        vuln_vals = [r["value"] for r in vuln_rows if r["value"] is not None]

        if not gini_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for Gini coefficient SI.POV.GINI",
            }

        gini_latest = gini_vals[0]
        # Gini trend: recent vs older window
        gini_trend = round(gini_vals[0] - gini_vals[-1], 3) if len(gini_vals) > 1 else None
        trend_direction = (
            "worsening" if gini_trend is not None and gini_trend > 1.0
            else "improving" if gini_trend is not None and gini_trend < -1.0
            else "stable"
        )

        vuln_employment = vuln_vals[0] if vuln_vals else None

        # Base score from Gini level
        if gini_latest < 30:
            base = 10.0
        elif gini_latest < 38:
            base = 10.0 + (gini_latest - 30) * 2.5
        elif gini_latest < 46:
            base = 30.0 + (gini_latest - 38) * 3.0
        elif gini_latest < 55:
            base = 54.0 + (gini_latest - 46) * 2.5
        else:
            base = min(95.0, 76.5 + (gini_latest - 55) * 1.5)

        # Worsening trend amplifies polarization stress
        if trend_direction == "worsening":
            base = min(100.0, base + 10.0)
        elif trend_direction == "improving":
            base = max(5.0, base - 8.0)

        # High vulnerable employment signals workers most exposed to automation shock
        if vuln_employment is not None:
            if vuln_employment >= 50:
                base = min(100.0, base + 8.0)
            elif vuln_employment <= 15:
                base = max(5.0, base - 5.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "gini_latest": round(gini_latest, 2),
                "gini_trend_change": gini_trend,
                "trend_direction": trend_direction,
                "vulnerable_employment_pct": round(vuln_employment, 2) if vuln_employment is not None else None,
                "n_obs_gini": len(gini_vals),
                "n_obs_vulnerable": len(vuln_vals),
                "polarization_accelerating": trend_direction == "worsening" and gini_latest > 38,
            },
        }
