"""Biopharmaceutical market: pharmaceutical R&D and biologics market dynamics.

Biopharmaceuticals -- monoclonal antibodies, vaccines, gene therapies, recombinant
proteins -- now constitute over 40% of new drug approvals globally. Countries with
strong pharmaceutical R&D investment, a growing health technology sector, and
accessible drug markets are positioned at the frontier of the bioeconomy.

Score: high pharmaceutical R&D + high health expenditure + rising healthcare access
-> STABLE strong biopharma ecosystem, low R&D with high disease burden and poor
access -> CRISIS (market failure in biopharmaceuticals).

Proxies: health expenditure (% GDP) as biopharma market size proxy, R&D (% GDP)
for innovation capacity, pharmaceuticals high-tech exports where available.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class BiopharmaceuticalMarket(LayerBase):
    layer_id = "lBI"
    name = "Biopharmaceutical Market"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        health_code = "SH.XPD.CHEX.GD.ZS"
        rnd_code = "GB.XPD.RSDV.GD.ZS"
        hitech_code = "TX.VAL.TECH.MF.ZS"

        health_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (health_code, "%health expenditure%"),
        )
        rnd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rnd_code, "%research and development%"),
        )
        hitech_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (hitech_code, "%High-technology exports%"),
        )

        health_vals = [r["value"] for r in health_rows if r["value"] is not None]
        rnd_vals = [r["value"] for r in rnd_rows if r["value"] is not None]
        hitech_vals = [r["value"] for r in hitech_rows if r["value"] is not None]

        if not health_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for health expenditure SH.XPD.CHEX.GD.ZS",
            }

        health_gdp = health_vals[0]
        rnd_gdp = rnd_vals[0] if rnd_vals else None
        hitech_pct = hitech_vals[0] if hitech_vals else None

        # Base score from health expenditure (market size proxy)
        # Higher health spend with good R&D = strong biopharma market (lower stress)
        if health_gdp >= 10:
            base = 15.0
        elif health_gdp >= 6:
            base = 20.0 + (10 - health_gdp) * 2.0
        elif health_gdp >= 3:
            base = 28.0 + (6 - health_gdp) * 4.0
        else:
            base = min(80.0, 40.0 + (3 - health_gdp) * 8.0)

        # R&D adjusts: high R&D improves biopharma capacity
        if rnd_gdp is not None:
            if rnd_gdp >= 2.5:
                base = max(5.0, base - 18.0)
            elif rnd_gdp >= 1.5:
                base = max(5.0, base - 10.0)
            elif rnd_gdp >= 0.8:
                base = max(5.0, base - 5.0)
            elif rnd_gdp < 0.3:
                base = min(100.0, base + 12.0)

        # High-tech export share signals competitive biopharma manufacturing
        if hitech_pct is not None:
            if hitech_pct >= 20:
                base = max(5.0, base - 8.0)
            elif hitech_pct < 5:
                base = min(100.0, base + 5.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "health_expenditure_gdp_pct": round(health_gdp, 2),
                "rnd_gdp_pct": round(rnd_gdp, 2) if rnd_gdp is not None else None,
                "hitech_exports_pct": round(hitech_pct, 2) if hitech_pct is not None else None,
                "n_obs_health": len(health_vals),
                "n_obs_rnd": len(rnd_vals),
                "n_obs_hitech": len(hitech_vals),
            },
        }
