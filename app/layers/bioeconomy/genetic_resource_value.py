"""Genetic resource value: biodiversity wealth and genetic resource economic potential.

Genetic resources -- plant varieties, animal breeds, microbial strains -- underpin
agriculture, medicine, and industrial biotechnology. Countries with high biodiversity
hold natural capital stocks whose value is reflected in agricultural productivity,
pharmaceutical pipelines, and bio-prospecting royalties.

Score: high forest cover + high terrestrial protected area + low species threat rate
-> STABLE rich genetic endowment, declining forest and high threat rate -> CRISIS
(genetic erosion undermining future bioeconomy potential).

Proxies: forest area (% of land), terrestrial protected areas (% of land), and
species threat approximated via IUCN-derived pressures inferred from deforestation rate.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class GeneticResourceValue(LayerBase):
    layer_id = "lBI"
    name = "Genetic Resource Value"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        forest_code = "AG.LND.FRST.ZS"
        protect_code = "ER.LND.PTLD.ZS"

        forest_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (forest_code, "%Forest area%"),
        )
        protect_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (protect_code, "%Terrestrial protected%"),
        )

        forest_vals = [r["value"] for r in forest_rows if r["value"] is not None]
        protect_vals = [r["value"] for r in protect_rows if r["value"] is not None]

        if not forest_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for forest area AG.LND.FRST.ZS",
            }

        forest_pct = forest_vals[0]
        protected_pct = protect_vals[0] if protect_vals else None

        # Forest trend (deforestation pressure)
        forest_trend = round(forest_vals[0] - forest_vals[-1], 3) if len(forest_vals) > 1 else None

        # Base score: lower forest cover = higher genetic erosion risk
        if forest_pct >= 50:
            base = 10.0
        elif forest_pct >= 30:
            base = 10.0 + (50 - forest_pct) * 1.0
        elif forest_pct >= 15:
            base = 30.0 + (30 - forest_pct) * 1.5
        elif forest_pct >= 5:
            base = 52.5 + (15 - forest_pct) * 2.0
        else:
            base = min(90.0, 72.5 + (5 - forest_pct) * 2.5)

        # Protected area modifies: high protection reduces erosion risk
        if protected_pct is not None:
            if protected_pct >= 20:
                base = max(5.0, base - 12.0)
            elif protected_pct >= 10:
                base = max(5.0, base - 6.0)
            elif protected_pct < 3:
                base = min(100.0, base + 8.0)

        # Deforestation trend worsens score
        if forest_trend is not None and forest_trend < -1.0:
            base = min(100.0, base + 10.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "forest_area_pct": round(forest_pct, 2),
                "protected_area_pct": round(protected_pct, 2) if protected_pct is not None else None,
                "forest_trend_pct": forest_trend,
                "n_obs_forest": len(forest_vals),
                "n_obs_protected": len(protect_vals),
                "genetic_erosion_risk": "high" if base >= 50 else "moderate" if base >= 25 else "low",
            },
        }
