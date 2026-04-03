"""Synthetic biology economics: industrial biotech investment and bio-manufacturing value added.

Synthetic biology engineers biological systems for industrial applications: bio-based
chemicals, bio-fuels, engineered microbes for material production, and programmable
organisms for environmental remediation. The economic value is realized through
bio-manufacturing replacing petrochemical processes and creating new product categories.

Score: high industrial R&D + rising high-tech manufacturing share -> STABLE
(synthetic biology delivering economic value), stagnant investment with low
manufacturing sophistication -> STRESS (lagging behind the bio-manufacturing frontier).

Proxies: R&D expenditure (% GDP) as innovation investment, high-technology exports
(% of manufactured exports) as bio-manufacturing output signal.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SyntheticBiologyEconomics(LayerBase):
    layer_id = "lBI"
    name = "Synthetic Biology Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        rnd_code = "GB.XPD.RSDV.GD.ZS"
        hitech_code = "TX.VAL.TECH.MF.ZS"
        manuf_code = "NV.IND.MANF.ZS"

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
        manuf_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (manuf_code, "%Manufacturing, value added%"),
        )

        rnd_vals = [r["value"] for r in rnd_rows if r["value"] is not None]
        hitech_vals = [r["value"] for r in hitech_rows if r["value"] is not None]
        manuf_vals = [r["value"] for r in manuf_rows if r["value"] is not None]

        if not rnd_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for R&D expenditure GB.XPD.RSDV.GD.ZS",
            }

        rnd_gdp = rnd_vals[0]
        hitech_pct = hitech_vals[0] if hitech_vals else None
        manuf_gdp = manuf_vals[0] if manuf_vals else None

        # Base score from R&D: synthetic biology is R&D-intensive
        # Low R&D = country outside the synthetic biology frontier
        if rnd_gdp >= 3.0:
            base = 8.0
        elif rnd_gdp >= 2.0:
            base = 15.0 + (3.0 - rnd_gdp) * 10.0
        elif rnd_gdp >= 1.0:
            base = 25.0 + (2.0 - rnd_gdp) * 20.0
        elif rnd_gdp >= 0.5:
            base = 45.0 + (1.0 - rnd_gdp) * 20.0
        else:
            base = min(90.0, 55.0 + (0.5 - rnd_gdp) * 30.0)

        # High-tech exports signal bio-manufacturing capability
        if hitech_pct is not None:
            if hitech_pct >= 25:
                base = max(5.0, base - 15.0)
            elif hitech_pct >= 15:
                base = max(5.0, base - 8.0)
            elif hitech_pct < 5:
                base = min(100.0, base + 10.0)

        # Manufacturing value added: bio-manufacturing requires industrial base
        if manuf_gdp is not None:
            if manuf_gdp >= 20:
                base = max(5.0, base - 5.0)
            elif manuf_gdp < 8:
                base = min(100.0, base + 5.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "rnd_gdp_pct": round(rnd_gdp, 2),
                "hitech_exports_pct": round(hitech_pct, 2) if hitech_pct is not None else None,
                "manufacturing_value_added_gdp_pct": round(manuf_gdp, 2) if manuf_gdp is not None else None,
                "n_obs_rnd": len(rnd_vals),
                "n_obs_hitech": len(hitech_vals),
                "n_obs_manuf": len(manuf_vals),
                "synbio_frontier": rnd_gdp >= 2.0 and (hitech_pct or 0) >= 15,
            },
        }
