"""Currency Mismatch Risk module.

Measures the gap between FX-denominated external debt liabilities and the
country's capacity to generate FX revenues (exports + remittances). A large
mismatch amplifies the debt burden when the domestic currency depreciates.

Methodology:
- Query DT.DOD.DECT.GD.ZS (external debt, % GNI) as a proxy for FX liabilities.
- Query BX.TRF.PWKR.DT.GD.ZS (personal remittances received, % GDP).
- Query NE.EXP.GNFS.ZS (exports of goods and services, % GDP).
- FX coverage = (exports_pct + remittances_pct) / external_debt_pct.
- Score = clip(max(0, 1.5 - fx_coverage) / 1.5 * 100, 0, 100).

Sources: World Bank WDI
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CurrencyMismatchRisk(LayerBase):
    layer_id = "lXD"
    name = "Currency Mismatch Risk"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        debt_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'DT.DOD.DECT.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        export_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.EXP.GNFS.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        remit_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BX.TRF.PWKR.DT.GD.ZS'
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not debt_rows or not export_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient FX mismatch data"}

        debt_map = {r["date"]: float(r["value"]) for r in debt_rows if r["value"] is not None}
        export_map = {r["date"]: float(r["value"]) for r in export_rows if r["value"] is not None}
        remit_map = {r["date"]: float(r["value"]) for r in remit_rows if r["value"] is not None}

        common = sorted(set(debt_map) & set(export_map), reverse=True)
        if not common:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no overlapping dates"}

        ref_date = common[0]
        debt_pct = debt_map[ref_date]
        export_pct = export_map[ref_date]
        remit_pct = remit_map.get(ref_date, 0.0)

        if debt_pct <= 0:
            return {"score": None, "signal": "UNAVAILABLE", "error": "debt value zero or negative"}

        fx_coverage = (export_pct + remit_pct) / debt_pct
        score = float(np.clip(max(0.0, 1.5 - fx_coverage) / 1.5 * 100, 0, 100))

        return {
            "score": round(score, 1),
            "country": country,
            "fx_coverage_ratio": round(fx_coverage, 4),
            "external_debt_pct_gni": round(debt_pct, 2),
            "exports_pct_gdp": round(export_pct, 2),
            "remittances_pct_gdp": round(remit_pct, 2),
            "reference_date": ref_date,
            "mismatch_high": fx_coverage < 0.5,
            "indicators": ["DT.DOD.DECT.GD.ZS", "NE.EXP.GNFS.ZS", "BX.TRF.PWKR.DT.GD.ZS"],
        }
