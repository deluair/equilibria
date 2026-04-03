"""Veteran economic integration: veteran unemployment and wage gap indicators.

Veterans returning to the civilian labor market face structural mismatches
between military skills and civilian demand. This module uses total unemployment
(SL.UEM.TOTL.ZS) as a baseline and military employment share of labor force
(MS.MIL.TOTL.TF.ZS) as a proxy for the veteran population flow rate.

Higher military share combined with high general unemployment signals likely
veteran reintegration stress. Countries with robust active labor market policies
show lower veteran unemployment premiums.

Score: low unemployment + low military share -> STABLE reintegration,
high unemployment + high military churn -> STRESS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class VeteranEconomicIntegration(LayerBase):
    layer_id = "lDX"
    name = "Veteran Economic Integration"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        uem_code = "SL.UEM.TOTL.ZS"
        mil_code = "MS.MIL.TOTL.TF.ZS"

        uem_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (uem_code, "%unemployment%total%"),
        )
        mil_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (mil_code, "%armed forces%labor%"),
        )

        uem_vals = [r["value"] for r in uem_rows if r["value"] is not None]
        mil_vals = [r["value"] for r in mil_rows if r["value"] is not None]

        if not uem_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for unemployment SL.UEM.TOTL.ZS",
            }

        uem = uem_vals[0]
        mil_share = mil_vals[0] if mil_vals else None

        # Base score from unemployment
        if uem < 4.0:
            base = 10.0
        elif uem < 7.0:
            base = 10.0 + (uem - 4.0) * 5.0
        elif uem < 12.0:
            base = 25.0 + (uem - 7.0) * 5.0
        elif uem < 20.0:
            base = 50.0 + (uem - 12.0) * 3.0
        else:
            base = min(95.0, 74.0 + (uem - 20.0) * 1.5)

        # Adjust for military churn: large armed forces as % labor force implies more veterans
        if mil_share is not None and mil_share > 1.0:
            base = min(100.0, base + (mil_share - 1.0) * 3.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "unemployment_rate_pct": round(uem, 3),
                "military_labor_share_pct": round(mil_share, 3) if mil_share is not None else None,
                "n_obs_uem": len(uem_vals),
                "n_obs_mil": len(mil_vals),
                "reintegration_risk": (
                    "low" if score < 25 else "moderate" if score < 50 else "high" if score < 75 else "critical"
                ),
            },
        }
