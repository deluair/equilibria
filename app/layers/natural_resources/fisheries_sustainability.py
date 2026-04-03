"""Fisheries sustainability: fish stock health and overexploitation.

Uses World Bank WDI:
  ER.FSH.PROD.MT   - capture fisheries production (metric tons)
  AG.PRD.FISH.MT   - aquaculture production (metric tons) [if available]
  TX.VAL.FISH.ZS.UN - fish exports (% merchandise exports)

Proxy overexploitation indicator: rapid growth in capture volume relative to
historical peak signals stock pressure. High fish export dependency amplifies risk.

Score:
  s_export_dep = clip(fish_export_pct * 2, 0, 50)  [export concentration risk]
  s_production = 0 (growth moderate) to 50 (production plateaued or declining after peak)
  score = clip(s_export_dep + s_production, 0, 100)

Sources: World Bank WDI (ER.FSH.PROD.MT, TX.VAL.FISH.ZS.UN)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class FisheriesSustainability(LayerBase):
    layer_id = "lNR"
    name = "Fisheries Sustainability"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN (
                'ER.FSH.PROD.MT', 'TX.VAL.FISH.ZS.UN', 'AG.PRD.FISH.MT'
            )
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 60
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no fisheries data",
            }

        # Collect time series for capture production
        production_series: list[tuple[str, float]] = []
        export_pct: float | None = None
        aquaculture_mt: float | None = None

        for r in rows:
            if r["value"] is None:
                continue
            sid = r["series_id"]
            val = float(r["value"])
            yr = r["date"][:4]
            if sid == "ER.FSH.PROD.MT":
                production_series.append((yr, val))
            elif sid == "TX.VAL.FISH.ZS.UN" and export_pct is None:
                export_pct = val
            elif sid == "AG.PRD.FISH.MT" and aquaculture_mt is None:
                aquaculture_mt = val

        if not production_series and export_pct is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient fisheries indicator data",
            }

        # Export dependency sub-score
        s_export = float(np.clip((export_pct or 0.0) * 2.0, 0, 50))

        # Production trend sub-score: declining from peak = high stress
        s_production = 0.0
        production_latest = None
        production_peak = None
        decline_pct = None
        if len(production_series) >= 3:
            vals = [v for _, v in sorted(production_series, key=lambda x: x[0])]
            production_latest = vals[-1]
            production_peak = max(vals)
            if production_peak > 0:
                decline_pct = (production_peak - production_latest) / production_peak * 100
                s_production = float(np.clip(max(0.0, decline_pct) * 1.0, 0, 50))

        score = float(np.clip(s_export + s_production, 0, 100))

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "capture_production_latest_mt": (
                    round(production_latest, 0) if production_latest is not None else None
                ),
                "capture_production_peak_mt": (
                    round(production_peak, 0) if production_peak is not None else None
                ),
                "production_decline_from_peak_pct": (
                    round(decline_pct, 2) if decline_pct is not None else None
                ),
                "fish_export_pct_merchandise": (
                    round(export_pct, 3) if export_pct is not None else None
                ),
                "aquaculture_latest_mt": (
                    round(aquaculture_mt, 0) if aquaculture_mt is not None else None
                ),
                "sub_scores": {
                    "export_dependency": round(s_export, 2),
                    "production_decline": round(s_production, 2),
                },
                "n_production_years": len(production_series),
            },
        }
