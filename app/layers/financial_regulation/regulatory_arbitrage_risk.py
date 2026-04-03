"""Regulatory Arbitrage Risk.

Measures the risk that actors exploit differences between domestic regulations
and global standards. Proxied by FDI inflows as % of GDP (BX.KLT.DINV.WD.GD.ZS)
and trade openness (NE.TRD.GNFS.ZS). High openness with high FDI relative to
economic size can indicate regulatory arbitrage attraction.

Score (0-100): composite openness-driven arbitrage risk.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class RegulatoryArbitrageRisk(LayerBase):
    layer_id = "lFR"
    name = "Regulatory Arbitrage Risk"

    async def compute(self, db, **kwargs) -> dict:
        results = {}
        indicators = {
            "fdi_inflows_gdp": ("BX.KLT.DINV.WD.GD.ZS", "foreign direct investment net inflows"),
            "trade_openness": ("NE.TRD.GNFS.ZS", "trade in goods and services"),
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
                "error": "no regulatory arbitrage proxy data found",
            }

        score_parts = []
        if "fdi_inflows_gdp" in results:
            # FDI > 20% GDP signals potential offshore center behavior
            fdi = abs(results["fdi_inflows_gdp"])
            score_parts.append(min(100.0, fdi / 20.0 * 100.0))
        if "trade_openness" in results:
            # Trade > 200% GDP typical for entrepot/offshore centers
            trade = results["trade_openness"]
            score_parts.append(min(100.0, trade / 200.0 * 100.0))

        score = float(sum(score_parts) / len(score_parts))

        return {
            "score": round(score, 2),
            "fdi_inflows_pct_gdp": results.get("fdi_inflows_gdp"),
            "trade_openness_pct_gdp": results.get("trade_openness"),
            "indicators_found": len(results),
            "interpretation": self._interpret(score),
        }

    @staticmethod
    def _interpret(score: float) -> str:
        if score >= 75:
            return "high arbitrage risk: structural features attract regulatory shopping"
        if score >= 50:
            return "elevated openness: some regulatory arbitrage incentives present"
        if score >= 25:
            return "moderate openness: limited arbitrage incentive"
        return "low arbitrage risk: economy not structurally prone to regulatory shopping"
