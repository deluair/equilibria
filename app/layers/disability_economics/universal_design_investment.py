"""Universal design investment: ROI of universal design in public investment.

Universal design -- building infrastructure and services accessible from the
outset -- yields positive returns by expanding productive participation and
reducing retrofit costs. Proxied by gross fixed capital formation as a share
of GDP (NE.GDI.FTOT.ZS) and government effectiveness (GE.EST, WGI). High
investment with strong governance suggests capacity to embed accessibility
standards; low investment or weak governance signals a universal design gap.

Score: high GFCF + strong governance -> STABLE investment embedding accessibility.
Low GFCF + weak governance -> CRISIS systemic underinvestment in universal design.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class UniversalDesignInvestment(LayerBase):
    layer_id = "lDI"
    name = "Universal Design Investment"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gfcf_code = "NE.GDI.FTOT.ZS"
        gov_code = "GE.EST"

        gfcf_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gfcf_code, "%gross fixed capital%"),
        )
        gov_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gov_code, "%government effectiveness%"),
        )

        gfcf_vals = [r["value"] for r in gfcf_rows if r["value"] is not None]
        gov_vals = [r["value"] for r in gov_rows if r["value"] is not None]

        if not gfcf_vals and not gov_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no data for GFCF or government effectiveness"}

        if gfcf_vals and gov_vals:
            gfcf = gfcf_vals[0]
            gov_est = gov_vals[0]  # WGI: -2.5 (worst) to +2.5 (best)
            # High GFCF + high gov effectiveness -> low gap score
            gfcf_norm = min(1.0, gfcf / 35.0)  # ~35% GDP is high investment
            gov_norm = min(1.0, max(0.0, (gov_est + 2.5) / 5.0))  # 0=worst,1=best
            # Gap: invert both -- low investment and low governance = high gap
            gap = 1.0 - (gfcf_norm * 0.5 + gov_norm * 0.5)
            score = round(gap * 100.0, 2)
            return {
                "score": score,
                "signal": self.classify_signal(score),
                "metrics": {
                    "gross_fixed_capital_gdp_pct": round(gfcf, 2),
                    "government_effectiveness_est": round(gov_est, 3),
                    "universal_design_gap_index": round(gap, 4),
                    "n_obs_gfcf": len(gfcf_vals),
                    "n_obs_gov": len(gov_vals),
                },
            }

        if gfcf_vals:
            gfcf = gfcf_vals[0]
            gap = max(0.0, 1.0 - gfcf / 35.0)
            score = round(gap * 100.0, 2)
            return {
                "score": score,
                "signal": self.classify_signal(score),
                "metrics": {
                    "gross_fixed_capital_gdp_pct": round(gfcf, 2),
                    "government_effectiveness_est": None,
                    "n_obs_gfcf": len(gfcf_vals),
                },
            }

        gov_est = gov_vals[0]
        gov_norm = min(1.0, max(0.0, (gov_est + 2.5) / 5.0))
        gap = 1.0 - gov_norm
        score = round(gap * 100.0, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "gross_fixed_capital_gdp_pct": None,
                "government_effectiveness_est": round(gov_est, 3),
                "n_obs_gov": len(gov_vals),
            },
        }
