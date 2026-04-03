"""Stranded asset risk: NY.GDP.TOTL.RT.ZS fossil fuel dependence.

Methodology
-----------
Stranded asset risk measures how exposed an economy is to the devaluation of
fossil fuel assets as the energy transition accelerates. Countries with high
natural resource rents concentrated in fossil fuels face the greatest risk.

NY.GDP.TOTL.RT.ZS (total natural resource rents, % GDP) is used as a proxy for
fossil fuel wealth dependence. While this includes non-fossil resources (forestry,
minerals), it is the best single WDI indicator for resource-rent exposure.

    stranded_risk = total_resource_rents_pct_gdp (latest value)

A country earning >15% of GDP in natural resource rents and showing no
diversification trend faces severe stranded asset exposure (IRRA/IRENA 2022).

Score: 0 = <1% resource rents (minimal exposure), 100 = >20% rents (extreme exposure).

Sources: World Bank WDI NY.GDP.TOTL.RT.ZS (total natural resources rents, % GDP).
IRENA Stranded Assets and Renewables (2017).
IMF Fiscal Monitor: Managing the Fiscal Consequences of Natural Disasters and Climate Change.
"""

from __future__ import annotations

from app.layers.base import LayerBase

_CODE = "NY.GDP.TOTL.RT.ZS"
_NAME = "Total natural resources rents"


class StrandedAssetRisk(LayerBase):
    layer_id = "lGT"
    name = "Stranded Asset Risk"

    # Score breakpoints (% of GDP in resource rents)
    LOW_RISK = 1.0     # below this: minimal exposure
    HIGH_RISK = 20.0   # above this: extreme exposure

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 5",
            (_CODE, f"%{_NAME}%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no resource rents data (NY.GDP.TOTL.RT.ZS)"}

        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        if not vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid resource rents values"}

        latest_rents = vals[0]

        # Linear mapping: 1% -> 0 score, 20% -> 100 score
        if latest_rents <= self.LOW_RISK:
            score = 0.0
        elif latest_rents >= self.HIGH_RISK:
            score = 100.0
        else:
            score = (latest_rents - self.LOW_RISK) / (self.HIGH_RISK - self.LOW_RISK) * 100

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "resource_rents_pct_gdp": round(latest_rents, 3),
                "low_risk_threshold_pct": self.LOW_RISK,
                "high_risk_threshold_pct": self.HIGH_RISK,
                "observations": len(vals),
            },
        }
