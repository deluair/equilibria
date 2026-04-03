"""Health R&D investment: total R&D spending as proxy for health R&D.

Total R&D expenditure as a share of GDP (GB.XPD.RSDV.GD.ZS) is used as a
proxy for health R&D investment in the absence of health-specific R&D data.
Higher R&D investment correlates with stronger pharmaceutical innovation capacity.

Key references:
    Røttingen, J.A. et al. (2013). Mapping of available health research and
        development data. The Lancet, 382(9900), 1286-1307.
    Viergever, R.F. & Hendriks, T.C. (2016). The 10 largest public and
        philanthropic funders of health research in the world. Health Research
        Policy and Systems, 14(1).
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class HealthRdInvestment(LayerBase):
    layer_id = "lPH"
    name = "Health R&D Investment"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate health R&D investment capacity from total R&D % GDP.

        Uses GB.XPD.RSDV.GD.ZS (R&D expenditure as % of GDP). Low R&D
        spending signals limited pharmaceutical innovation capacity.
        Score rises with R&D underinvestment relative to a 2% benchmark.

        Returns dict with score, signal, and relevant metrics.
        """
        code = "GB.XPD.RSDV.GD.ZS"
        name = "research and development expenditure"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"No data for {code} in DB",
            }

        values = [float(row["value"]) for row in rows if row["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All fetched rows have NULL value",
            }

        latest = values[0]
        mean_val = float(np.mean(values))

        # Benchmark: 2% GDP R&D is considered adequate. Below = stress.
        # Score = max(0, (2 - latest) / 2) * 100 -> low R&D = high score (stress)
        score = float(np.clip(((2.0 - latest) / 2.0) * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "rd_pct_gdp_latest": round(latest, 3),
                "rd_pct_gdp_mean_15obs": round(mean_val, 3),
                "n_observations": len(values),
                "indicator": code,
                "benchmark_pct_gdp": 2.0,
            },
        }
