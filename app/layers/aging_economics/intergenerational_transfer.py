"""Intergenerational transfer: old-age dependency + transfers burden.

Intergenerational transfer systems redistribute income from working-age
to elderly populations through PAYGO pensions, healthcare, and social
protection. The combined burden of old-age dependency ratio and government
transfer spending as a share of GDP captures the fiscal cost of these
transfers and intergenerational equity implications.

High dependency + high transfers signals a heavy intergenerational burden
that may crowd out investment in education, infrastructure, and youth.

Score: dependency > 30 + transfers > 15% GDP -> CRISIS, low both -> STABLE.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class IntergenerationalTransfer(LayerBase):
    layer_id = "lAG"
    name = "Intergenerational Transfer"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        dep_code = "SP.POP.DPND.OL"
        trft_code = "GC.XPN.TRFT.ZS"

        dep_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (dep_code, "%old-age dependency%"),
        )
        trft_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (trft_code, "%transfers%"),
        )

        dep_vals = [r["value"] for r in dep_rows if r["value"] is not None]
        trft_vals = [r["value"] for r in trft_rows if r["value"] is not None]

        if not dep_vals and not trft_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for old-age dependency or transfers",
            }

        dependency = dep_vals[0] if dep_vals else None
        transfers = trft_vals[0] if trft_vals else None

        if dependency is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for old-age dependency ratio SP.POP.DPND.OL",
            }

        # Composite burden index
        # Dependency contributes 60%, transfers 40%
        dep_norm = min(100.0, dependency / 50.0 * 100.0)  # 50+ dependency -> max
        trft_norm = min(100.0, transfers / 20.0 * 100.0) if transfers is not None else 50.0

        burden_index = 0.6 * dep_norm + 0.4 * trft_norm

        score = round(burden_index, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "old_age_dependency_ratio": round(dependency, 2),
                "transfer_spending_gdp_pct": round(transfers, 2) if transfers is not None else None,
                "dependency_normalized": round(dep_norm, 2),
                "transfers_normalized": round(trft_norm, 2),
                "burden_index": round(burden_index, 2),
                "n_obs_dependency": len(dep_vals),
                "n_obs_transfers": len(trft_vals),
            },
        }
