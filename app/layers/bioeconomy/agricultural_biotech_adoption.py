"""Agricultural biotech adoption: GMO crop adoption as productivity multiplier.

Biotechnology adoption in agriculture -- including GM crop varieties, precision
breeding, and bio-inoculants -- raises yields per hectare, reduces pesticide
costs, and improves drought/pest resilience. High adoption with rising cereal
yields signals that biotech is functioning as a productivity multiplier.

Score: stagnant yields with low R&D investment -> STRESS (biotech gap),
rising yields alongside agricultural R&D -> STABLE (adoption working),
high yield volatility despite investment -> WATCH (adoption risk).

Proxies: cereal yield (kg/ha) as biotech outcome, agricultural R&D spending
approximated via GERD (gross expenditure on R&D) where sectoral data absent.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AgriculturalBiotechAdoption(LayerBase):
    layer_id = "lBI"
    name = "Agricultural Biotech Adoption"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        yield_code = "AG.YLD.CREL.KG"
        rnd_code = "GB.XPD.RSDV.GD.ZS"

        yield_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (yield_code, "%Cereal yield%"),
        )
        rnd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rnd_code, "%research and development%"),
        )

        yield_vals = [r["value"] for r in yield_rows if r["value"] is not None]
        rnd_vals = [r["value"] for r in rnd_rows if r["value"] is not None]

        if not yield_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for cereal yield AG.YLD.CREL.KG",
            }

        latest_yield = yield_vals[0]
        rnd_gdp = rnd_vals[0] if rnd_vals else None

        # Yield trend over available window
        trend = round(yield_vals[0] - yield_vals[-1], 1) if len(yield_vals) > 1 else None

        # Base score from yield level (kg/ha); global median ~3,500
        if latest_yield >= 6000:
            base = 10.0  # high yield -> strong adoption
        elif latest_yield >= 4000:
            base = 20.0 + (6000 - latest_yield) / 200.0
        elif latest_yield >= 2500:
            base = 30.0 + (4000 - latest_yield) / 150.0
        elif latest_yield >= 1500:
            base = 50.0 + (2500 - latest_yield) / 100.0
        else:
            base = min(85.0, 60.0 + (1500 - latest_yield) / 50.0)

        # Negative trend with low R&D -> push score higher (more stress)
        if trend is not None and trend < 0:
            base = min(100.0, base + 10.0)
        if rnd_gdp is not None:
            if rnd_gdp >= 2.0:
                base = max(5.0, base - 10.0)
            elif rnd_gdp < 0.5:
                base = min(100.0, base + 8.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "cereal_yield_kg_ha": round(latest_yield, 1),
                "yield_trend_kg_ha": trend,
                "rnd_gdp_pct": round(rnd_gdp, 2) if rnd_gdp is not None else None,
                "n_obs_yield": len(yield_vals),
                "n_obs_rnd": len(rnd_vals),
            },
        }
