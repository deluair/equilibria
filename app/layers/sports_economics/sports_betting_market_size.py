"""Sports betting market size: gambling industry value added proxy.

Financial services value added (WDI FS.AST.DOMS.GD.ZS) and recreation
services share jointly proxy the gambling and sports wagering sector.
Formal sports betting is tightly correlated with financial sector depth
(payment infrastructure, credit access) and recreation industry scale.
Rapid financial deepening in economies with liberalizing gambling
regulations signals accelerating sports betting market expansion.

Score: low financial depth + low recreation -> STABLE regulated/suppressed
market; rising financial depth -> WATCH; high depth with large recreation
share -> STRESS (addiction externalities); extreme values -> CRISIS
(systemic integrity risk to sports competitions).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SportsBettingMarketSize(LayerBase):
    layer_id = "lSP"
    name = "Sports Betting Market Size"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        fin_code = "FS.AST.DOMS.GD.ZS"
        rec_code = "IS.SRV.MISC.ZS"

        fin_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (fin_code, "%domestic credit%"),
        )
        rec_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rec_code, "%recreation%"),
        )

        fin_vals = [r["value"] for r in fin_rows if r["value"] is not None]
        rec_vals = [r["value"] for r in rec_rows if r["value"] is not None]

        if not fin_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for FS.AST.DOMS.GD.ZS",
            }

        fin = fin_vals[0]
        rec = rec_vals[0] if rec_vals else 1.0

        # Betting market proxy: financial depth (% GDP) * recreation share
        # Normalize: typical range 10-300% for fin, 0.5-5% for rec
        betting_proxy = (fin / 100.0) * rec

        if betting_proxy < 0.5:
            score = 5.0 + betting_proxy * 30.0
        elif betting_proxy < 2.0:
            score = 20.0 + (betting_proxy - 0.5) * 20.0
        elif betting_proxy < 5.0:
            score = 50.0 + (betting_proxy - 2.0) * 8.3
        else:
            score = min(100.0, 75.0 + (betting_proxy - 5.0) * 3.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "domestic_credit_gdp_pct": round(fin, 2),
                "recreation_services_pct": round(rec, 3),
                "betting_market_proxy": round(betting_proxy, 4),
                "n_obs_fin": len(fin_vals),
                "n_obs_rec": len(rec_vals),
            },
        }
