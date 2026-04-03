"""Sports public health dividend: physical activity and reduced health costs.

Mortality rate from non-communicable diseases (WDI SH.DYN.NCOM.ZS) and
health expenditure per capita (SH.XPD.CHEX.PC.CD) together capture the
fiscal benefit of population-level physical activity. Economies with
widespread sports participation exhibit lower NCD mortality and more
contained per-capita health costs -- the public health dividend of sport.
A high NCD mortality rate combined with high per-capita health spend signals
a missed dividend: low physical activity driving preventable disease cost.

Score: compound burden index from NCD mortality and health expenditure.
Low burden -> STABLE (dividend being captured); moderate -> WATCH;
high burden -> STRESS (dividend not realized); extreme -> CRISIS
(preventable disease driving fiscal healthcare overload).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SportsPublicHealthDividend(LayerBase):
    layer_id = "lSP"
    name = "Sports Public Health Dividend"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        ncd_code = "SH.DYN.NCOM.ZS"
        hexp_code = "SH.XPD.CHEX.PC.CD"

        ncd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (ncd_code, "%noncommunicable%"),
        )
        hexp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (hexp_code, "%health expenditure per capita%"),
        )

        ncd_vals = [r["value"] for r in ncd_rows if r["value"] is not None]
        hexp_vals = [r["value"] for r in hexp_rows if r["value"] is not None]

        if not ncd_vals and not hexp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SH.DYN.NCOM.ZS or SH.XPD.CHEX.PC.CD",
            }

        ncd = ncd_vals[0] if ncd_vals else 50.0   # % of total deaths from NCDs
        hexp = hexp_vals[0] if hexp_vals else 500.0  # USD per capita

        # Normalize health expenditure to 0-100 scale (reference: $10,000 max)
        hexp_norm = min(hexp / 100.0, 100.0)

        # Burden index: average of NCD mortality share and normalized health spend
        burden = (ncd + hexp_norm) / 2.0

        if burden < 20.0:
            score = 5.0 + burden * 0.75
        elif burden < 40.0:
            score = 20.0 + (burden - 20.0) * 1.5
        elif burden < 65.0:
            score = 50.0 + (burden - 40.0) * 1.0
        else:
            score = min(100.0, 75.0 + (burden - 65.0) * 0.71)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "ncd_mortality_share_pct": round(ncd, 2),
                "health_expenditure_per_capita_usd": round(hexp, 2),
                "health_burden_index": round(burden, 3),
                "n_obs_ncd": len(ncd_vals),
                "n_obs_hexp": len(hexp_vals),
                "dividend_status": (
                    "captured" if burden < 20.0
                    else "partial" if burden < 40.0
                    else "missed" if burden < 65.0
                    else "absent"
                ),
            },
        }
