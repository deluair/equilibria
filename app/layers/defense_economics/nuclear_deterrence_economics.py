"""Nuclear deterrence economics: defense burden analysis for nuclear-armed states.

Nuclear arsenals require sustained investment in warheads, delivery systems,
command and control infrastructure, and safety measures. The Congressional
Budget Office (2023) estimates US nuclear forces cost $756B over 2023-2032.
This module identifies nuclear burden by combining military expenditure as % GDP
(MS.MIL.XPND.GD.ZS) with R&D expenditure (GB.XPD.RSDV.GD.ZS) as a proxy for
nuclear modernization investment.

Nuclear-armed states (US, Russia, China, UK, France, India, Pakistan, Israel, DPRK)
typically carry heavier combined military + advanced R&D burdens.

Score: low combined burden -> STABLE, high combined burden -> STRESS (opportunity cost).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class NuclearDeterrenceEconomics(LayerBase):
    layer_id = "lDX"
    name = "Nuclear Deterrence Economics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        mil_code = "MS.MIL.XPND.GD.ZS"
        rnd_code = "GB.XPD.RSDV.GD.ZS"

        mil_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (mil_code, "%military expenditure%GDP%"),
        )
        rnd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (rnd_code, "%research and development%GDP%"),
        )

        mil_vals = [r["value"] for r in mil_rows if r["value"] is not None]
        rnd_vals = [r["value"] for r in rnd_rows if r["value"] is not None]

        if not mil_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for military expenditure MS.MIL.XPND.GD.ZS",
            }

        mil = mil_vals[0]
        rnd = rnd_vals[0] if rnd_vals else None

        # Combined burden: military spend + a fraction of R&D (defense R&D ~30-50% of total)
        if rnd is not None:
            combined_burden = mil + (rnd * 0.4)
        else:
            combined_burden = mil

        # Score: opportunity cost of deterrence investment
        if combined_burden < 2.0:
            score = 10.0
        elif combined_burden < 3.5:
            score = 10.0 + (combined_burden - 2.0) * 10.0
        elif combined_burden < 5.5:
            score = 25.0 + (combined_burden - 3.5) * 12.5
        elif combined_burden < 8.0:
            score = 50.0 + (combined_burden - 5.5) * 10.0
        else:
            score = min(100.0, 75.0 + (combined_burden - 8.0) * 5.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "military_spending_gdp_pct": round(mil, 3),
                "rnd_gdp_pct": round(rnd, 3) if rnd is not None else None,
                "combined_deterrence_burden_pct": round(combined_burden, 3),
                "n_obs_mil": len(mil_vals),
                "n_obs_rnd": len(rnd_vals),
                "burden_level": (
                    "minimal" if combined_burden < 2.0
                    else "moderate" if combined_burden < 3.5
                    else "elevated" if combined_burden < 5.5
                    else "heavy" if combined_burden < 8.0
                    else "extreme"
                ),
            },
        }
