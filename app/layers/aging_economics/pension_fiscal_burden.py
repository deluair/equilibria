"""Pension fiscal burden: old-age dependency x government transfer spending.

Old-age dependency ratio measures the number of elderly (65+) per 100
working-age persons (15-64). Combined with government transfer spending as
a share of GDP, it reveals the fiscal burden of pension and social security
obligations. High dependency with high transfers indicates unsustainable
PAYGO systems.

Aaron (1966) condition: PAYGO pension sustainable only when population growth
plus productivity growth exceeds the real interest rate.

Score: high dependency + high transfers -> CRISIS, low dependency + low
transfers -> STABLE.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class PensionFiscalBurden(LayerBase):
    layer_id = "lAG"
    name = "Pension Fiscal Burden"
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

        if not dep_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for old-age dependency ratio SP.POP.DPND.OL",
            }

        dependency = dep_vals[0]
        transfers = trft_vals[0] if trft_vals else None

        # Base score from dependency ratio
        if dependency < 10:
            base = 10.0
        elif dependency < 20:
            base = 10.0 + (dependency - 10) * 2.0
        elif dependency < 30:
            base = 30.0 + (dependency - 20) * 2.5
        elif dependency < 45:
            base = 55.0 + (dependency - 30) * 2.0
        else:
            base = min(95.0, 85.0 + (dependency - 45) * 1.0)

        # Augment with transfers: high transfers add pressure
        if transfers is not None:
            if transfers > 15:
                base = min(100.0, base + 10.0)
            elif transfers > 8:
                base = min(100.0, base + 5.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "old_age_dependency_ratio": round(dependency, 2),
                "transfer_spending_gdp_pct": round(transfers, 2) if transfers is not None else None,
                "n_obs_dependency": len(dep_vals),
                "n_obs_transfers": len(trft_vals),
                "fiscal_stress": score > 50,
            },
        }
