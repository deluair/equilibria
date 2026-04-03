"""Media pluralism index: composite of press freedom, access, and ownership diversity.

Media pluralism requires three conditions: regulatory freedom to operate
independently, physical and economic access to reach audiences, and diverse
ownership preventing monopolistic control over narratives. This composite
combines governance quality (proxy for press freedom), internet access
(proxy for distribution pluralism), and regulatory environment to produce
a single pluralism stress score.

Score: high governance + high access + open regulatory environment -> STABLE;
moderate on all dimensions -> WATCH; weak governance or low access -> STRESS;
authoritarian media environment with access monopolies -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MediaPluralismIndex(LayerBase):
    layer_id = "lMD"
    name = "Media Pluralism Index"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gov_code = "IQ.CPA.IRAI.XQ"  # CPIA overall rating
        net_code = "IT.NET.USER.ZS"
        reg_code = "IQ.REG.QUAL.XQ"  # Regulatory quality (WGI)

        gov_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gov_code, "%CPIA%"),
        )
        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (net_code, "%internet users%"),
        )
        reg_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (reg_code, "%regulatory quality%"),
        )

        net_vals = [r["value"] for r in net_rows if r["value"] is not None]
        gov_vals = [r["value"] for r in gov_rows if r["value"] is not None]
        reg_vals = [r["value"] for r in reg_rows if r["value"] is not None]

        if not net_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for internet users IT.NET.USER.ZS",
            }

        net_pct = net_vals[0]
        gov_score = gov_vals[0] if gov_vals else None
        reg_score = reg_vals[0] if reg_vals else None

        # Access dimension (0-100 scale from internet penetration)
        access_score = net_pct  # already 0-100

        # Governance dimension: CPIA 1-6 -> normalize to 0-100
        if gov_score is not None:
            gov_norm = ((gov_score - 1.0) / 5.0) * 100.0
        else:
            gov_norm = 40.0  # neutral fallback

        # Regulatory quality: WGI ranges roughly -2.5 to +2.5 -> normalize to 0-100
        if reg_score is not None:
            reg_norm = ((reg_score + 2.5) / 5.0) * 100.0
            reg_norm = max(0.0, min(100.0, reg_norm))
        else:
            reg_norm = 40.0  # neutral fallback

        # Pluralism composite: equal weight on access, governance, regulatory quality
        pluralism_norm = (access_score + gov_norm + reg_norm) / 3.0

        # Invert: high pluralism norm -> low stress score
        base = 100.0 - pluralism_norm

        score = round(max(5.0, min(95.0, base)), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "internet_users_pct": round(net_pct, 2),
                "governance_score_cpia": round(gov_score, 2) if gov_score is not None else None,
                "regulatory_quality_wgi": round(reg_score, 2) if reg_score is not None else None,
                "access_norm": round(access_score, 2),
                "governance_norm": round(gov_norm, 2),
                "regulatory_norm": round(reg_norm, 2),
                "pluralism_composite": round(pluralism_norm, 2),
                "n_obs_internet": len(net_vals),
                "n_obs_governance": len(gov_vals),
                "n_obs_regulatory": len(reg_vals),
            },
        }
