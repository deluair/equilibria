"""Copyright industry value: copyright-intensive industries economic contribution.

Copyright-intensive industries include software, publishing, film, music,
broadcasting, and design. Proxied by resident patent applications (IP.PAT.RESD)
as a signal of IP-productive capacity and ICT services exports (BX.GSR.CCIS.ZS)
as the dominant revenue stream for digital copyright industries.

Score: low patent + low ICT exports -> STABLE nascent IP economy,
moderate activity -> WATCH developing IP base, high -> STRESS significant
copyright industry but also enforcement complexity, very high -> CRISIS
potential IP concentration risk.
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase


class CopyrightIndustryValue(LayerBase):
    layer_id = "lAR"
    name = "Copyright Industry Value"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        pat_code = "IP.PAT.RESD"
        ict_code = "BX.GSR.CCIS.ZS"

        pat_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pat_code, "%patent applications%resident%"),
        )
        ict_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ict_code, "%ICT service%"),
        )

        pat_vals = [r["value"] for r in pat_rows if r["value"] is not None]
        ict_vals = [r["value"] for r in ict_rows if r["value"] is not None]

        if not pat_vals and not ict_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for IP.PAT.RESD or BX.GSR.CCIS.ZS",
            }

        pat_latest = pat_vals[0] if pat_vals else 0.0
        ict_latest = ict_vals[0] if ict_vals else 0.0

        # Log-scale patent count: raw values span 1 to 1,000,000+
        pat_log = math.log10(pat_latest + 1) if pat_latest > 0 else 0.0
        # Normalize: log10(1M) = 6 -> max reference
        pat_norm = min(100.0, pat_log / 6.0 * 100.0)

        # ICT services exports as % of services: 0-50%+ range
        ict_norm = min(100.0, ict_latest / 50.0 * 100.0)

        # Combined score: 50% weight each
        score = 0.5 * pat_norm + 0.5 * ict_norm

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "patent_applications_resident": round(pat_latest, 0),
                "patent_log10": round(pat_log, 3),
                "ict_services_exports_pct": round(ict_latest, 3),
                "patent_norm_score": round(pat_norm, 2),
                "ict_norm_score": round(ict_norm, 2),
                "n_obs_patents": len(pat_vals),
                "n_obs_ict": len(ict_vals),
            },
        }
