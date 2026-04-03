"""Currency swap networks: bilateral swap line coverage as liquidity backstop.

Bilateral central bank swap lines (e.g. Federal Reserve swap network, PBOC
bilateral swaps) have emerged as a critical international lender-of-last-resort
mechanism since the 2008 GFC. Countries outside the major swap networks face
dollar liquidity cliffs during crises. This module proxies swap network
coverage via external debt service ratios and short-term debt exposure,
since bilateral swap data is not in WDI.

Obstfeld et al. (2010): swap lines act as insurance against dollar funding
shortfalls; their absence amplifies currency crisis risk.

Score: low short-term external debt + manageable debt service -> STABLE;
high short-term exposure with no swap backstop -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class CurrencySwapNetworks(LayerBase):
    layer_id = "lMS"
    name = "Currency Swap Networks"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # Short-term debt as % of total external debt: WDI DT.DOD.DSTC.ZS
        st_code = "DT.DOD.DSTC.ZS"
        st_name = "Short-term debt"
        st_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (st_code, f"%{st_name}%"),
        )
        # Debt service as % of GNI: WDI DT.TDS.DECT.GN.ZS
        ds_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("DT.TDS.DECT.GN.ZS", "%debt service%"),
        )

        st_vals = [r["value"] for r in st_rows if r["value"] is not None]
        ds_vals = [r["value"] for r in ds_rows if r["value"] is not None]

        if not st_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for DT.DOD.DSTC.ZS",
            }

        st_debt = st_vals[0]
        debt_service = ds_vals[0] if ds_vals else None

        # Base score from short-term debt share
        if st_debt < 10:
            base = 10.0
        elif st_debt < 20:
            base = 10.0 + (st_debt - 10) * 2.0
        elif st_debt < 35:
            base = 30.0 + (st_debt - 20) * 2.5
        elif st_debt < 50:
            base = 67.5 + (st_debt - 35) * 1.5
        else:
            base = min(95.0, 90.0 + (st_debt - 50) * 0.5)

        # Debt service burden augmentation
        if debt_service is not None:
            if debt_service > 20:
                base = min(100.0, base + 10.0)
            elif debt_service > 10:
                base = min(100.0, base + 5.0)

        score = round(base, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "short_term_debt_share_pct": round(st_debt, 2),
                "debt_service_gni_pct": round(debt_service, 2) if debt_service is not None else None,
                "n_obs_st": len(st_vals),
                "n_obs_ds": len(ds_vals),
                "swap_backstop_need": "high" if score > 50 else "moderate" if score > 25 else "low",
            },
        }
