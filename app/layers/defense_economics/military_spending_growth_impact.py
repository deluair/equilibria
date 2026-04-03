"""Military spending growth impact: crowding-out vs stimulus effect on GDP growth.

Compares military expenditure growth rate with GDP growth rate to assess whether
defense spending acts as a fiscal stimulus or crowds out productive investment.

When military spending growth consistently exceeds GDP growth, it signals resource
diversion. Benoit (1978) found a positive correlation for developing nations;
Dunne and Tian (2015) found predominantly negative crowding-out effects.

Score: spending growth <= GDP growth -> STABLE, moderate excess -> WATCH,
large excess -> STRESS, extreme divergence -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MilitarySpendingGrowthImpact(LayerBase):
    layer_id = "lDX"
    name = "Military Spending Growth Impact"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        mil_code = "MS.MIL.XPND.ZS"  # Military expenditure % of central govt expenditure
        gdp_code = "NY.GDP.MKTP.KD.ZG"  # GDP growth (annual %)

        mil_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (mil_code, "%military expenditure%central%"),
        )
        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (gdp_code, "%GDP growth%annual%"),
        )

        mil_vals = [r["value"] for r in mil_rows if r["value"] is not None]
        gdp_vals = [r["value"] for r in gdp_rows if r["value"] is not None]

        if not gdp_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for GDP growth NY.GDP.MKTP.KD.ZG"}

        gdp_latest = gdp_vals[0]

        if not mil_vals:
            # Fallback: use GDP growth alone as a partial signal
            score = max(0.0, min(100.0, 50.0 - gdp_latest * 3.0))
            return {
                "score": round(score, 2),
                "signal": self.classify_signal(score),
                "metrics": {
                    "gdp_growth_pct": round(gdp_latest, 3),
                    "military_share_govt_pct": None,
                    "crowdout_signal": None,
                    "n_obs_gdp": len(gdp_vals),
                    "note": "military share of govt expenditure unavailable",
                },
            }

        mil_latest = mil_vals[0]
        # Crowding-out proxy: high military share of govt spending combined with low GDP growth
        crowdout = mil_latest - (gdp_latest * 2.0)  # synthetic divergence measure

        if crowdout < 0:
            score = 15.0
        elif crowdout < 5:
            score = 15.0 + crowdout * 3.0
        elif crowdout < 15:
            score = 30.0 + (crowdout - 5) * 2.5
        elif crowdout < 25:
            score = 55.0 + (crowdout - 15) * 2.0
        else:
            score = min(100.0, 75.0 + (crowdout - 25) * 1.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "gdp_growth_pct": round(gdp_latest, 3),
                "military_share_govt_pct": round(mil_latest, 3),
                "crowdout_index": round(crowdout, 3),
                "n_obs_gdp": len(gdp_vals),
                "n_obs_military": len(mil_vals),
                "interpretation": (
                    "stimulus" if crowdout < 0
                    else "neutral" if crowdout < 5
                    else "mild crowding-out" if crowdout < 15
                    else "strong crowding-out"
                ),
            },
        }
