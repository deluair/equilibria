"""Stadium infrastructure economics: public investment in sports facilities.

Gross fixed capital formation in government services (WDI NE.GDI.GOVT.ZS)
proxies the public sector's capital allocation envelope from which sports
facility investment is drawn. Higher public capital formation combined with
a large recreation services share indicates greater latent stadium investment.
Countries spending <15% of GDP on public GFCF tend to under-invest in sports
infrastructure; those above 30% risk fiscal overextension.

Score: composite of public GFCF (% GDP) weighted against recreation share.
Low public investment -> STABLE low exposure; rising investment without
corresponding receipts growth -> WATCH misallocation risk; high public GFCF +
large recreation share -> STRESS; extreme capital intensity -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class StadiumInfrastructureEconomics(LayerBase):
    layer_id = "lSP"
    name = "Stadium Infrastructure Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gfcf_code = "NE.GDI.GOVT.ZS"
        rec_code = "IS.SRV.MISC.ZS"

        gfcf_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gfcf_code, "%government capital%"),
        )
        rec_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rec_code, "%recreation%"),
        )

        gfcf_vals = [r["value"] for r in gfcf_rows if r["value"] is not None]
        rec_vals = [r["value"] for r in rec_rows if r["value"] is not None]

        if not gfcf_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for NE.GDI.GOVT.ZS",
            }

        gfcf = gfcf_vals[0]
        rec = rec_vals[0] if rec_vals else 1.0  # default 1% if missing

        # Composite: public capex intensity * recreation weight
        composite = gfcf * (rec / 2.0)

        if composite < 5.0:
            score = 10.0 + composite * 2.5
        elif composite < 15.0:
            score = 22.5 + (composite - 5.0) * 2.75
        elif composite < 30.0:
            score = 50.0 + (composite - 15.0) * 1.67
        else:
            score = min(100.0, 75.0 + (composite - 30.0) * 0.83)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "public_gfcf_gdp_pct": round(gfcf, 2),
                "recreation_services_pct": round(rec, 3),
                "infrastructure_composite": round(composite, 3),
                "n_obs_gfcf": len(gfcf_vals),
                "n_obs_rec": len(rec_vals),
            },
        }
