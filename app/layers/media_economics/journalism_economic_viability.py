"""Journalism economic viability: press freedom and media employment conditions.

The economic viability of journalism depends on two reinforcing conditions:
a legal/political environment that permits free reporting, and a labor market
that sustains professional journalists. Press freedom deterioration correlates
with loss of advertising revenue, self-censorship, and outlet closures. GDP
per capita proxies for the depth of the advertising and subscription market
available to sustain news organizations.

Score: high press freedom + strong income base -> STABLE; moderate freedom
with thin markets -> WATCH; low freedom or very weak income base -> STRESS;
severe press restrictions with no economic base for journalism -> CRISIS.
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase


class JournalismEconomicViability(LayerBase):
    layer_id = "lMD"
    name = "Journalism Economic Viability"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gdp_code = "NY.GDP.PCAP.PP.KD"
        freedom_code = "IQ.CPA.PUBS.XQ"  # CPIA transparency and accountability

        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gdp_code, "%GDP per capita%"),
        )
        freedom_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (freedom_code, "%transparency%"),
        )

        gdp_vals = [r["value"] for r in gdp_rows if r["value"] is not None]
        freedom_vals = [r["value"] for r in freedom_rows if r["value"] is not None]

        if not gdp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for GDP per capita NY.GDP.PCAP.PP.KD",
            }

        gdp_pc = gdp_vals[0]
        freedom_score = freedom_vals[0] if freedom_vals else None

        # GDP per capita -> market depth for journalism (log scale)
        # Reference: $2k (subsistence), $10k (emerging), $30k (developed media market)
        gdp_log = math.log10(max(gdp_pc, 100))
        gdp_log_max = math.log10(80000)
        gdp_log_min = math.log10(500)
        gdp_norm = (gdp_log - gdp_log_min) / (gdp_log_max - gdp_log_min)
        gdp_norm = max(0.0, min(1.0, gdp_norm))

        # Higher GDP = more viable journalism market -> lower stress score
        base = 80.0 - gdp_norm * 60.0

        # Transparency/accountability index (CPIA scale 1-6): higher = better governance
        if freedom_score is not None:
            # Normalize 1-6 scale: 1=worst, 6=best
            freedom_norm = (freedom_score - 1.0) / 5.0
            freedom_norm = max(0.0, min(1.0, freedom_norm))
            # High freedom reduces stress, low freedom amplifies it
            base = base - freedom_norm * 15.0 + (1.0 - freedom_norm) * 10.0

        score = round(max(5.0, min(100.0, base)), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "gdp_per_capita_ppp": round(gdp_pc, 0),
                "gdp_market_depth_norm": round(gdp_norm, 4),
                "transparency_index": round(freedom_score, 2) if freedom_score is not None else None,
                "n_obs_gdp": len(gdp_vals),
                "n_obs_freedom": len(freedom_vals),
                "viable_market": gdp_pc >= 10000 and (freedom_score is None or freedom_score >= 3.0),
            },
        }
