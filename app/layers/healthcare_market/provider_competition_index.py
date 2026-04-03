"""Provider competition index.

Proxies market competitiveness using private health expenditure share.
A high private share can indicate a competitive market with multiple
providers, or a fragmented unregulated market depending on context.
Combined with OOP burden, it signals whether private spending reflects
insurance or direct payment.

Key references:
    Gaynor, M., Ho, K. & Town, R. (2015). The industrial organization of
        health care markets. Journal of Economic Literature, 53(2), 235-284.
    World Bank WDI: SH.XPD.PVTD.CH.ZS.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class ProviderCompetitionIndex(LayerBase):
    layer_id = "lHM"
    name = "Provider Competition Index"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate provider competition from private health expenditure mix.

        Moderate private share (30-60%) suggests competitive pluralism.
        Very high (>80%) or very low (<10%) both signal structural issues.
        """
        code = "SH.XPD.PVTD.CH.ZS"

        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{code}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No private health expenditure data in DB",
            }

        vals = [float(r["value"]) for r in rows if r["value"] is not None]
        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid private health expenditure values",
            }

        mean_pvt = float(np.mean(vals))

        # Optimal band: 30-60% private => score near 0 (competitive)
        # Outside band => stress increases
        if 30.0 <= mean_pvt <= 60.0:
            deviation = 0.0
        elif mean_pvt < 30.0:
            deviation = (30.0 - mean_pvt) / 30.0
        else:
            deviation = (mean_pvt - 60.0) / 40.0

        score = float(np.clip(deviation * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "mean_private_health_share_pct": round(mean_pvt, 2),
                "optimal_band_low": 30.0,
                "optimal_band_high": 60.0,
                "n_obs": len(vals),
            },
        }
