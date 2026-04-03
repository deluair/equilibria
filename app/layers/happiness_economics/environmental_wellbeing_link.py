"""Environmental wellbeing link: air quality and green space as wellbeing determinants.

Exposure to air pollution (PM2.5, NO2) reduces life satisfaction, increases
mortality, and imposes large welfare costs (OECD Environmental Outlook). Green
space access and low particulate exposure are among the strongest environmental
predictors of subjective wellbeing. WHO guidelines set PM2.5 < 5 ug/m3 as safe;
values above 35 are associated with significant health burden.

This module proxies environmental wellbeing via WDI air pollution indicators:
mean annual exposure to PM2.5 (EN.ATM.PM25.MC.M3) and access to clean fuels
for cooking (EG.CFT.ACCS.ZS), which also reflects indoor air quality.

Score: low PM2.5 + high clean fuel access -> STABLE, high PM2.5 + low clean fuel
-> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase

# WHO 2021 guideline: 5 ug/m3. WHO interim targets: 10, 15, 25, 35.
_WHO_SAFE = 5.0
_WHO_CRISIS = 35.0


class EnvironmentalWellbeingLink(LayerBase):
    layer_id = "lHE"
    name = "Environmental Wellbeing Link"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        pm25_code = "EN.ATM.PM25.MC.M3"
        fuel_code = "EG.CFT.ACCS.ZS"

        pm25_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (pm25_code, "%PM2.5%"),
        )
        fuel_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (fuel_code, "%clean fuels%cooking%"),
        )

        pm25_vals = [r["value"] for r in pm25_rows if r["value"] is not None]
        fuel_vals = [r["value"] for r in fuel_rows if r["value"] is not None]

        if not pm25_vals and not fuel_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for EN.ATM.PM25.MC.M3 or EG.CFT.ACCS.ZS",
            }

        score_parts = []

        if pm25_vals:
            pm25 = pm25_vals[0]
            # Map PM2.5 to stress score: 5->10, 15->30, 25->55, 35->75, 50+->95
            if pm25 <= _WHO_SAFE:
                pm_score = 5.0 + pm25 * 1.0
            elif pm25 <= 15:
                pm_score = 10.0 + (pm25 - 5) * 2.0
            elif pm25 <= 25:
                pm_score = 30.0 + (pm25 - 15) * 2.5
            elif pm25 <= _WHO_CRISIS:
                pm_score = 55.0 + (pm25 - 25) * 2.0
            else:
                pm_score = min(100.0, 75.0 + (pm25 - 35) * 1.2)
            score_parts.append(pm_score)

        if fuel_vals:
            fuel_access = fuel_vals[0]
            # Clean fuel access: 0% -> 85 (crisis), 100% -> 5 (stable)
            fuel_score = max(5.0, 85.0 - fuel_access * 0.8)
            score_parts.append(fuel_score)

        score = sum(score_parts) / len(score_parts)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "pm25_ug_m3": round(pm25_vals[0], 2) if pm25_vals else None,
                "clean_fuel_access_pct": round(fuel_vals[0], 2) if fuel_vals else None,
                "who_pm25_guideline": _WHO_SAFE,
                "env_tier": (
                    "clean"
                    if score < 25
                    else "moderate"
                    if score < 50
                    else "polluted"
                    if score < 75
                    else "hazardous"
                ),
                "n_obs_pm25": len(pm25_vals),
                "n_obs_fuel": len(fuel_vals),
            },
        }
