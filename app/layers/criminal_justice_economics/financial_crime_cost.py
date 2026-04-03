"""Financial crime cost: AML compliance burden and fraud losses via banking stability proxy.

Financial crime (money laundering, fraud, embezzlement, cyber fraud) imposes direct
losses and large compliance costs on the banking system. The FATF estimates global
money laundering at 2-5% of GDP. Banks spend 1-3% of revenue on AML compliance.
Weaker banking systems with lower capital adequacy are more vulnerable to financial
crime penetration and correspondent banking de-risking.

Score: strong banking stability + low NPLs (resilient to financial crime) -> STABLE,
moderate vulnerabilities -> WATCH, weak banking system -> STRESS,
fragile system with high fraud exposure -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class FinancialCrimeCost(LayerBase):
    layer_id = "lCJ"
    name = "Financial Crime Cost"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # WDI: Bank nonperforming loans to total gross loans (%)
        npl_code = "FB.AST.NPER.ZS"
        npl_name = "nonperforming loans"

        # WDI: Bank capital to total assets (%)
        cap_code = "FB.BNK.CAPA.ZS"
        cap_name = "bank capital"

        npl_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (npl_code, f"%{npl_name}%"),
        )
        cap_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (cap_code, f"%{cap_name}%"),
        )

        npl_vals = [r["value"] for r in npl_rows if r["value"] is not None]
        cap_vals = [r["value"] for r in cap_rows if r["value"] is not None]

        if not npl_vals and not cap_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for financial crime proxy (NPL/bank capital)",
            }

        if npl_vals:
            npl = npl_vals[0]
            # Higher NPLs indicate weaker system more susceptible to financial crime
            if npl < 2:
                base = 8.0 + npl * 3.0
            elif npl < 5:
                base = 14.0 + (npl - 2) * 6.0
            elif npl < 15:
                base = 32.0 + (npl - 5) * 3.5
            elif npl < 30:
                base = 67.0 + (npl - 15) * 1.5
            else:
                base = min(100.0, 89.5 + (npl - 30) * 0.5)
        else:
            # Use capital adequacy: lower capital = higher risk
            cap = cap_vals[0]
            base = max(5.0, min(95.0, 95.0 - cap * 2.5))

        # Adjust for capital buffer if available
        if npl_vals and cap_vals:
            cap = cap_vals[0]
            if cap < 5:
                base = min(100.0, base + 10.0)
            elif cap > 15:
                base = max(0.0, base - 5.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "nonperforming_loans_pct": round(npl_vals[0], 2) if npl_vals else None,
                "bank_capital_to_assets_pct": round(cap_vals[0], 2) if cap_vals else None,
                "n_obs_npl": len(npl_vals),
                "n_obs_capital": len(cap_vals),
                "financial_crime_vulnerability": (
                    "low" if score < 25
                    else "moderate" if score < 50
                    else "high" if score < 75
                    else "severe"
                ),
            },
        }
