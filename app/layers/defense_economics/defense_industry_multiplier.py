"""Defense industry multiplier: economic multiplier effects of defense spending.

The defense spending multiplier captures how $1 of military expenditure
translates into broader GDP output. Barro (1981) and Hall (2009) estimate
US defense multipliers of 0.6-1.0; Ramey (2011) finds 1.2 in the short run.

This module proxies the multiplier using the ratio of value added in manufacturing
(as % GDP) to military spending (as % GDP). Higher manufacturing base relative
to military spend suggests a larger domestic defense industrial base with stronger
multiplier effects.

Score: high multiplier (strong domestic base) -> STABLE,
low multiplier (import-dependent) -> elevated stress signal.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DefenseIndustryMultiplier(LayerBase):
    layer_id = "lDX"
    name = "Defense Industry Multiplier"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        mfg_code = "NV.IND.MANF.ZS"  # Manufacturing value added % GDP
        mil_code = "MS.MIL.XPND.GD.ZS"

        mfg_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (mfg_code, "%manufacturing%value added%"),
        )
        mil_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (mil_code, "%military expenditure%GDP%"),
        )

        mfg_vals = [r["value"] for r in mfg_rows if r["value"] is not None]
        mil_vals = [r["value"] for r in mil_rows if r["value"] is not None]

        if not mfg_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for manufacturing value added NV.IND.MANF.ZS",
            }

        mfg = mfg_vals[0]
        mil = mil_vals[0] if mil_vals else 2.0

        # Multiplier proxy: manufacturing base per unit of defense spending
        multiplier_proxy = mfg / max(mil, 0.1)

        # Higher multiplier_proxy -> more domestic industrial capacity -> STABLE
        if multiplier_proxy >= 8.0:
            score = 10.0
        elif multiplier_proxy >= 5.0:
            score = 10.0 + (8.0 - multiplier_proxy) * 5.0
        elif multiplier_proxy >= 3.0:
            score = 25.0 + (5.0 - multiplier_proxy) * 7.5
        elif multiplier_proxy >= 1.5:
            score = 40.0 + (3.0 - multiplier_proxy) * 10.0
        else:
            score = min(100.0, 55.0 + (1.5 - multiplier_proxy) * 20.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "manufacturing_gdp_pct": round(mfg, 3),
                "military_spending_gdp_pct": round(mil, 3),
                "multiplier_proxy": round(multiplier_proxy, 3),
                "n_obs_mfg": len(mfg_vals),
                "n_obs_mil": len(mil_vals),
                "industrial_base": (
                    "strong" if multiplier_proxy >= 8.0
                    else "moderate" if multiplier_proxy >= 5.0
                    else "limited" if multiplier_proxy >= 3.0
                    else "weak"
                ),
            },
        }
