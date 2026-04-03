"""Aging innovation paradox: aging speed vs R&D investment.

The innovation paradox of aging posits that rapidly aging economies face
a dual challenge: declining innovation capacity (fewer young risk-takers,
less experimentation) while simultaneously needing more innovation to
compensate for shrinking workforces via automation and productivity gains.

Countries with high elderly share but low R&D investment are caught in
the paradox: they need innovation most, but invest least. High R&D with
moderate aging resolves the paradox via technology-led productivity.

Score: high elderly share + low R&D -> CRISIS, high R&D regardless of
aging -> STABLE (innovation offsets demographic headwinds).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AgingInnovationParadox(LayerBase):
    layer_id = "lAG"
    name = "Aging Innovation Paradox"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        pop_code = "SP.POP.65UP.TO.ZS"
        rnd_code = "GB.XPD.RSDV.GD.ZS"

        pop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pop_code, "%Population ages 65%"),
        )
        rnd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rnd_code, "%research and development%"),
        )

        pop_vals = [r["value"] for r in pop_rows if r["value"] is not None]
        rnd_vals = [r["value"] for r in rnd_rows if r["value"] is not None]

        if not pop_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for elderly population share SP.POP.65UP.TO.ZS",
            }

        elderly_share = pop_vals[0]
        rnd_gdp = rnd_vals[0] if rnd_vals else None

        # Base score from elderly share (aging pressure)
        if elderly_share < 7:
            base = 10.0
        elif elderly_share < 14:
            base = 15.0 + (elderly_share - 7) * 3.0
        elif elderly_share < 21:
            base = 36.0 + (elderly_share - 14) * 3.5
        else:
            base = min(80.0, 60.5 + (elderly_share - 21) * 2.0)

        # R&D investment modifies the paradox score
        # High R&D resolves paradox -> reduce stress
        # Low R&D deepens paradox -> increase stress
        if rnd_gdp is not None:
            if rnd_gdp >= 3.0:
                base = max(5.0, base - 20.0)  # strong innovation cushion
            elif rnd_gdp >= 2.0:
                base = max(5.0, base - 12.0)
            elif rnd_gdp >= 1.0:
                base = max(5.0, base - 5.0)
            elif rnd_gdp < 0.5:
                base = min(100.0, base + 15.0)  # paradox deepens

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "elderly_share_pct": round(elderly_share, 2),
                "rnd_gdp_pct": round(rnd_gdp, 2) if rnd_gdp is not None else None,
                "n_obs_pop": len(pop_vals),
                "n_obs_rnd": len(rnd_vals),
                "paradox_active": rnd_gdp is not None and rnd_gdp < 1.0 and elderly_share > 14,
                "innovation_adequate": rnd_gdp is not None and rnd_gdp >= 2.0,
            },
        }
