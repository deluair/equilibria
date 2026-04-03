"""Platform news subsidy: government and tech platform news funding mechanisms.

When market forces alone cannot sustain journalism, public subsidies and
platform-mandated contributions become critical. This module proxies news
funding adequacy via government ICT expenditure and R&D grants, which in
developed economies partially fund public broadcasting and digital news
infrastructure. Low public ICT investment with high platform concentration
signals an absence of compensating news subsidies.

Score: adequate public investment in digital infrastructure alongside media
sector -> STABLE; declining public investment -> WATCH; structural withdrawal
from public media support -> STRESS; no subsidy mechanism with market failure
-> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class PlatformNewsSubsidy(LayerBase):
    layer_id = "lMD"
    name = "Platform News Subsidy"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        rnd_code = "GB.XPD.RSDV.GD.ZS"
        gov_code = "GC.XPN.TOTL.GD.ZS"

        rnd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rnd_code, "%research and development%"),
        )
        gov_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gov_code, "%expense%"),
        )

        rnd_vals = [r["value"] for r in rnd_rows if r["value"] is not None]
        gov_vals = [r["value"] for r in gov_rows if r["value"] is not None]

        if not gov_vals and not rnd_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for government expenditure or R&D",
            }

        gov_pct = gov_vals[0] if gov_vals else None
        rnd_pct = rnd_vals[0] if rnd_vals else None

        # Public expenditure capacity as base signal for subsidy potential
        if gov_pct is not None:
            # High government expenditure (>30% GDP) allows room for media subsidies
            if gov_pct >= 30:
                base = 20.0
            elif gov_pct >= 20:
                base = 25.0 + (30 - gov_pct) * 1.5
            elif gov_pct >= 10:
                base = 40.0 + (20 - gov_pct) * 2.0
            else:
                base = min(100.0, 60.0 + (10 - gov_pct) * 3.0)
        else:
            base = 55.0  # unknown -> moderate stress

        # R&D investment signals innovation subsidy culture incl. media tech grants
        if rnd_pct is not None:
            if rnd_pct >= 2.0:
                base = max(10.0, base - 15.0)
            elif rnd_pct >= 1.0:
                base = max(10.0, base - 8.0)
            elif rnd_pct < 0.3:
                base = min(100.0, base + 10.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "gov_expenditure_gdp_pct": round(gov_pct, 2) if gov_pct is not None else None,
                "rnd_gdp_pct": round(rnd_pct, 2) if rnd_pct is not None else None,
                "n_obs_gov": len(gov_vals),
                "n_obs_rnd": len(rnd_vals),
                "subsidy_capacity_adequate": (
                    gov_pct is not None and gov_pct >= 20 and
                    rnd_pct is not None and rnd_pct >= 1.0
                ),
            },
        }
