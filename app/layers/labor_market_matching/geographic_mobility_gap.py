"""Geographic mobility gap: urban-rural migration flows vs regional unemployment differentials.

Geographic labor market segmentation prevents efficient matching when unemployed
workers in depressed regions cannot relocate to high-demand regions due to
housing costs, social ties, information gaps, or migration barriers. The gap
between urban and rural employment conditions proxies for this geographic
friction.

Score: narrow urban-rural gap -> STABLE mobility facilitates matching, moderate
gap -> WATCH emerging segmentation, large gap -> STRESS persistent regional
mismatch, extreme gap -> CRISIS geographically locked labor markets.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class GeographicMobilityGap(LayerBase):
    layer_id = "lLM"
    name = "Geographic Mobility Gap"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        urban_emp_code = "SL.UEM.TOTL.UR.ZS"
        rural_emp_code = "SL.UEM.TOTL.RU.ZS"
        urban_pop_code = "SP.URB.TOTL.IN.ZS"

        urban_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (urban_emp_code, "%unemployment%urban%"),
        )
        rural_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rural_emp_code, "%unemployment%rural%"),
        )
        urban_pop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (urban_pop_code, "%urban population%"),
        )

        urban_vals = [r["value"] for r in urban_rows if r["value"] is not None]
        rural_vals = [r["value"] for r in rural_rows if r["value"] is not None]
        urban_pop_vals = [r["value"] for r in urban_pop_rows if r["value"] is not None]

        if not urban_vals and not rural_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for urban/rural unemployment rates",
            }

        if urban_vals and rural_vals:
            gap = abs(urban_vals[0] - rural_vals[0])
            urban_unemp = urban_vals[0]
            rural_unemp = rural_vals[0]
        elif urban_vals:
            gap = urban_vals[0] * 0.5  # rough proxy: assume rural differs by half
            urban_unemp = urban_vals[0]
            rural_unemp = None
        else:
            gap = rural_vals[0] * 0.5
            urban_unemp = None
            rural_unemp = rural_vals[0]

        # Amplify gap by urbanization pace: rapid urbanization with high rural unemp = more friction
        urbanization_rate = urban_pop_vals[0] if urban_pop_vals else None

        if gap < 2:
            score = gap * 8.0
        elif gap < 5:
            score = 16.0 + (gap - 2) * 8.0
        elif gap < 10:
            score = 40.0 + (gap - 5) * 4.0
        else:
            score = min(100.0, 60.0 + (gap - 10) * 2.0)

        # Urbanization adjustment: very low urbanization with high rural unemp = more friction
        if urbanization_rate is not None and rural_unemp is not None:
            if urbanization_rate < 40 and rural_unemp > 10:
                score = min(100.0, score + 10.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "urban_unemployment_pct": round(urban_unemp, 2) if urban_unemp is not None else None,
                "rural_unemployment_pct": round(rural_unemp, 2) if rural_unemp is not None else None,
                "urban_rural_gap_pct": round(gap, 2),
                "urbanization_rate_pct": round(urbanization_rate, 2) if urbanization_rate is not None else None,
                "n_obs_urban": len(urban_vals),
                "n_obs_rural": len(rural_vals),
                "segmentation": "severe" if score > 50 else "moderate" if score > 25 else "low",
            },
        }
