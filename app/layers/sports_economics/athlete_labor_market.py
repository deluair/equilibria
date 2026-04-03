"""Athlete labor market: sports employment concentration and wage premium.

Arts, entertainment, and recreation employment share (WDI SL.SRV.EMPL.ZS
or ILO proxy) serves as the broadest available proxy for the formal sports
labor market. High employment concentration in this sector relative to total
services employment indicates a specialized sports-entertainment workforce
commanding a wage premium above the economy-wide median.

Score: very low share (<1%) -> STABLE nascent sector; moderate (1-3%) ->
WATCH growing professional class; high (3-6%) -> STRESS concentration with
wage inequality; very high (>6%) -> CRISIS structural dependence on volatile
entertainment revenues.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AthleteLaborMarket(LayerBase):
    layer_id = "lSP"
    name = "Athlete Labor Market"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "SL.SRV.EMPL.ZS"
        name = "services employment"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        values = [r["value"] for r in rows if r["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SL.SRV.EMPL.ZS",
            }

        latest = values[0]
        trend = round(values[0] - values[-1], 3) if len(values) > 1 else None

        # Arts/entertainment typically 2-8% of services employment.
        # Estimate sports sub-share at ~15% of that figure.
        sports_proxy = latest * 0.15

        if sports_proxy < 1.0:
            score = 8.0 + sports_proxy * 17.0
        elif sports_proxy < 3.0:
            score = 25.0 + (sports_proxy - 1.0) * 12.5
        elif sports_proxy < 6.0:
            score = 50.0 + (sports_proxy - 3.0) * 8.3
        else:
            score = min(100.0, 75.0 + (sports_proxy - 6.0) * 4.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "services_employment_share_pct": round(latest, 2),
                "sports_employment_proxy_pct": round(sports_proxy, 3),
                "trend_pct_change": trend,
                "n_obs": len(values),
            },
        }
