"""Global safe asset scarcity: safe asset supply vs. global savings glut.

Caballero, Farhi & Gourinchas (2008, 2017) argue that a chronic shortage
of safe assets -- primarily US Treasuries and equivalent instruments --
drives the global savings glut, low equilibrium interest rates, and
cross-border capital flow imbalances. This module proxies safe asset
scarcity via real interest rates (WDI FR.INR.RINR) and the spread between
nominal lending and deposit rates.

Low or negative real rates with compressed spreads signal excess demand
for safe assets; rising real rates signal normalization or safe asset
oversupply relative to demand.

Score: very low/negative real rates + compressed spreads -> STRESS (scarcity);
moderately positive real rates -> STABLE; sharply rising rates -> WATCH.
"""

from __future__ import annotations

import statistics

from app.layers.base import LayerBase


class GlobalSafeAssetScarcity(LayerBase):
    layer_id = "lMS"
    name = "Global Safe Asset Scarcity"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        code = "FR.INR.RINR"
        name = "Real interest rate"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )
        # Lending-deposit spread proxy
        spread_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("FR.INR.LNDP", "%interest rate spread%"),
        )

        vals = [r["value"] for r in rows if r["value"] is not None]
        spread_vals = [r["value"] for r in spread_rows if r["value"] is not None]

        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for FR.INR.RINR",
            }

        latest = vals[0]
        avg_real = round(statistics.mean(vals), 3)
        spread = spread_vals[0] if spread_vals else None

        # Score: very low/negative real rates = scarcity stress
        if latest > 5:
            # High real rates: potential tightening crisis
            score = 35.0 + min(30.0, (latest - 5) * 3.0)
        elif latest > 2:
            score = 15.0 + (latest - 2) * 6.7
        elif latest >= 0:
            score = 10.0 + (2 - latest) * 2.5
        elif latest >= -2:
            score = 15.0 + abs(latest) * 10.0
        else:
            score = 35.0 + min(40.0, abs(latest + 2) * 10.0)

        # Compressed spread reduces safe asset premium
        if spread is not None and spread < 2:
            score = min(100.0, score + 8.0)

        score = round(score, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "real_interest_rate_pct": round(latest, 2),
                "avg_real_rate_pct": avg_real,
                "interest_rate_spread": round(spread, 2) if spread is not None else None,
                "n_obs": len(vals),
                "safe_asset_regime": (
                    "scarcity" if latest < 0
                    else "low_yield" if latest < 2
                    else "normal" if latest < 5
                    else "elevated"
                ),
            },
        }
