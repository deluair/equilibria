"""Creative economy innovation: creative sector contribution to national innovation.

The creative economy feeds innovation through design thinking, cross-disciplinary
synthesis, and cultural content that diffuses ideas. Proxied by resident patent
applications (IP.PAT.RESD) as IP output and R&D expenditure as % of GDP
(GB.XPD.RSDV.GD.ZS) as the innovation investment base that co-produces creative
and technical knowledge.

Score: low R&D + low patents -> STABLE (pre-innovation economy), moderate ->
WATCH emerging innovation system, high -> STRESS intensive but potentially
brittle innovation reliance, very high -> CRISIS concentration of creative
capital with diminishing returns or exclusionary dynamics.
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase


class CreativeEconomyInnovation(LayerBase):
    layer_id = "lAR"
    name = "Creative Economy Innovation"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        pat_code = "IP.PAT.RESD"
        rnd_code = "GB.XPD.RSDV.GD.ZS"

        pat_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pat_code, "%patent applications%resident%"),
        )
        rnd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rnd_code, "%Research and development expenditure%"),
        )

        pat_vals = [r["value"] for r in pat_rows if r["value"] is not None]
        rnd_vals = [r["value"] for r in rnd_rows if r["value"] is not None]

        if not pat_vals and not rnd_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for IP.PAT.RESD or GB.XPD.RSDV.GD.ZS",
            }

        pat_latest = pat_vals[0] if pat_vals else 0.0
        rnd_latest = rnd_vals[0] if rnd_vals else 0.0

        rnd_trend = round(rnd_vals[0] - rnd_vals[-1], 3) if len(rnd_vals) > 1 else None

        # Patent normalization: log10 scale, reference max ~1M applications
        pat_log = math.log10(pat_latest + 1) if pat_latest > 0 else 0.0
        pat_norm = min(100.0, pat_log / 6.0 * 100.0)

        # R&D normalization: global range 0-5% GDP; >3% is high
        rnd_norm = min(100.0, rnd_latest / 5.0 * 100.0)

        # Combined: 40% patents (output), 60% R&D (investment base)
        score = 0.40 * pat_norm + 0.60 * rnd_norm

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "patent_applications_resident": round(pat_latest, 0),
                "rnd_expenditure_gdp_pct": round(rnd_latest, 3),
                "patent_log10": round(pat_log, 3),
                "patent_norm_score": round(pat_norm, 2),
                "rnd_norm_score": round(rnd_norm, 2),
                "trend_rnd_pct": rnd_trend,
                "n_obs_patents": len(pat_vals),
                "n_obs_rnd": len(rnd_vals),
            },
        }
