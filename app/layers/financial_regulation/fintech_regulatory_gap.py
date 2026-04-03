"""FinTech Regulatory Gap.

Measures the mismatch between innovation intensity (R&D spending, GB.XPD.RSDV.GD.ZS)
and regulatory quality (RQ.EST). High R&D with weak regulation implies a large
gap where fintech operates without adequate oversight.

Score (0-100): larger gap = higher regulatory risk.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class FintechRegulatoryGap(LayerBase):
    layer_id = "lFR"
    name = "FinTech Regulatory Gap"

    async def compute(self, db, **kwargs) -> dict:
        results = {}
        indicators = {
            "rd_spending": ("GB.XPD.RSDV.GD.ZS", "research and development expenditure"),
            "regulatory_quality": ("RQ.EST", "regulatory quality"),
        }

        for key, (code, name) in indicators.items():
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = ("
                "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (code, f"%{name}%"),
            )
            if rows:
                vals = [float(r["value"]) for r in rows if r["value"] is not None]
                if vals:
                    results[key] = vals[0]

        if not results:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no fintech regulatory gap data found",
            }

        # Normalize R&D (0-5% of GDP typical range -> 0-100)
        rd_norm = min(100.0, results.get("rd_spending", 0.0) / 5.0 * 100.0)
        # Normalize RQ: -2.5 to +2.5 -> 0-100 (higher RQ = lower reg gap risk)
        rq = results.get("regulatory_quality", 0.0)
        rq_norm = max(0.0, min(100.0, (-rq) * 20.0 + 50.0))

        if "rd_spending" in results and "regulatory_quality" in results:
            # Gap = high innovation + weak regulation
            score = float(max(0.0, min(100.0, (rd_norm + rq_norm) / 2.0)))
        elif "rd_spending" in results:
            score = rd_norm
        else:
            score = rq_norm

        return {
            "score": round(score, 2),
            "rd_spending_pct_gdp": results.get("rd_spending"),
            "regulatory_quality_est": results.get("regulatory_quality"),
            "indicators_found": len(results),
            "interpretation": self._interpret(score),
        }

    @staticmethod
    def _interpret(score: float) -> str:
        if score >= 75:
            return "large fintech regulatory gap: innovation outpacing oversight"
        if score >= 50:
            return "moderate gap: some fintech activity in regulatory grey zones"
        if score >= 25:
            return "small gap: regulation broadly keeping pace with innovation"
        return "minimal gap: strong regulatory coverage of fintech activities"
