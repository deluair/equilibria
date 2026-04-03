"""Loneliness economic cost: social isolation proxy and social capital indicators.

Social isolation and loneliness have been termed a public health epidemic by
the US Surgeon General (2023) and WHO. Economic costs include: reduced
productivity, higher healthcare utilization, premature mortality (loneliness
increases mortality risk by ~26%, Holt-Lunstad et al.), and reduced civic
participation. The UK estimates loneliness costs employers GBP 2.5 billion
annually.

This module proxies loneliness and social isolation using available WDI/WGI
indicators: single-person household rate as a structural isolation proxy
(proxied via urbanization + dependency ratio), and voice and accountability
WGI score (VA.EST) as a social capital / civic engagement proxy.

Score: high social capital + low isolation -> STABLE, low social capital +
high isolation indicators -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class LonelinessEconomicCost(LayerBase):
    layer_id = "lHE"
    name = "Loneliness Economic Cost"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        va_code = "VA.EST"  # Voice and accountability (WGI) -- civic/social participation
        urban_code = "SP.URB.TOTL.IN.ZS"  # Urbanization -- urban isolation proxy
        dep_code = "SP.POP.DPND"  # Age dependency ratio -- social support network proxy

        va_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (va_code, "%Voice and Accountability%"),
        )
        urban_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (urban_code, "%urban population%"),
        )
        dep_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (dep_code, "%age dependency ratio%"),
        )

        va_vals = [r["value"] for r in va_rows if r["value"] is not None]
        urban_vals = [r["value"] for r in urban_rows if r["value"] is not None]
        dep_vals = [r["value"] for r in dep_rows if r["value"] is not None]

        if not any([va_vals, urban_vals, dep_vals]):
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for VA.EST, SP.URB.TOTL.IN.ZS, or SP.POP.DPND",
            }

        score_parts = []

        if va_vals:
            # Voice & accountability: WGI -2.5 to +2.5
            # High VA = active civic life = lower loneliness proxy
            va = va_vals[0]
            va_score = max(5.0, min(100.0, 50.0 - va * 20.0))
            score_parts.append(va_score)

        if urban_vals:
            # Very high urbanization (>80%) without social infrastructure -> higher isolation risk
            # But moderate urbanization is neutral. Non-linear.
            urban = urban_vals[0]
            if urban < 40:
                u_score = 20.0
            elif urban < 65:
                u_score = 20.0 + (urban - 40) * 0.4
            elif urban < 80:
                u_score = 30.0 + (urban - 65) * 0.8
            else:
                u_score = 42.0 + (urban - 80) * 0.9
            score_parts.append(min(100.0, u_score))

        if dep_vals:
            # Low dependency ratio -> fewer multigenerational households -> more isolation
            dep = dep_vals[0]
            # Very low (<30): post-demographic transition, nuclear/solo households
            if dep < 30:
                d_score = 55.0 + (30 - dep) * 0.8
            elif dep < 50:
                d_score = 35.0 + (50 - dep) * 1.0
            elif dep < 70:
                d_score = 20.0 + (70 - dep) * 0.75
            else:
                d_score = max(10.0, 20.0 - (dep - 70) * 0.5)
            score_parts.append(min(100.0, d_score))

        score = sum(score_parts) / len(score_parts)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "voice_accountability_wgi": round(va_vals[0], 3) if va_vals else None,
                "urbanization_pct": round(urban_vals[0], 2) if urban_vals else None,
                "age_dependency_ratio": round(dep_vals[0], 2) if dep_vals else None,
                "isolation_tier": (
                    "low"
                    if score < 25
                    else "moderate"
                    if score < 50
                    else "elevated"
                    if score < 75
                    else "high"
                ),
                "n_obs_va": len(va_vals),
                "n_obs_urban": len(urban_vals),
                "n_obs_dep": len(dep_vals),
            },
        }
