"""Currency Competitiveness module.

Proxies real exchange rate overvaluation through the joint signal of
export volume growth and current account balance trends. Declining
exports combined with a widening current account deficit indicates
competitiveness loss, often linked to an overvalued exchange rate.

Score = clip(ca_component * 0.5 + export_component * 0.5, 0, 100)

Sources: WDI
  NE.EXP.GNFS.KD.ZG - Exports of goods and services (annual % growth)
  BN.CAB.XOKA.GD.ZS  - Current account balance (% of GDP)
"""

from __future__ import annotations

import numpy as np
from scipy.stats import linregress

from app.layers.base import LayerBase


class CurrencyCompetitiveness(LayerBase):
    layer_id = "lTP"
    name = "Currency Competitiveness"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        export_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NE.EXP.GNFS.KD.ZG'
            ORDER BY dp.date
            """,
            (country,),
        )

        ca_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'BN.CAB.XOKA.GD.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not export_rows and not ca_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no export or current account data",
            }

        export_growth = None
        export_slope = None
        if export_rows:
            exp_vals = [float(r["value"]) for r in export_rows if r["value"] is not None]
            if len(exp_vals) >= 3:
                export_growth = float(np.mean(exp_vals[-5:]))
                if len(exp_vals) >= 5:
                    t = np.arange(len(exp_vals), dtype=float)
                    export_slope, *_ = linregress(t, exp_vals)
                    export_slope = float(export_slope)

        ca_balance = None
        ca_slope = None
        if ca_rows:
            ca_vals = [float(r["value"]) for r in ca_rows if r["value"] is not None]
            if len(ca_vals) >= 3:
                ca_balance = float(np.mean(ca_vals[-5:]))
                if len(ca_vals) >= 5:
                    t = np.arange(len(ca_vals), dtype=float)
                    ca_slope, *_ = linregress(t, ca_vals)
                    ca_slope = float(ca_slope)

        # Export component: slow/negative growth -> competitiveness loss
        export_component = 0.0
        if export_growth is not None:
            export_component = float(np.clip(30 - export_growth * 3, 0, 60))

        # CA component: widening deficit -> competitiveness loss
        ca_component = 0.0
        if ca_balance is not None:
            # Negative CA (deficit) scores higher
            ca_component = float(np.clip(-ca_balance * 3 + 20, 0, 60))

        if export_growth is None and ca_balance is None:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid data"}

        if export_growth is None:
            score = ca_component
        elif ca_balance is None:
            score = export_component
        else:
            score = export_component * 0.5 + ca_component * 0.5

        score = float(np.clip(score, 0, 100))

        competitiveness = (
            "strong" if score < 25
            else "adequate" if score < 50
            else "weakening" if score < 75
            else "severely impaired"
        )

        return {
            "score": round(score, 1),
            "country": country,
            "recent_export_growth_pct": round(export_growth, 2) if export_growth is not None else None,
            "export_growth_trend_slope": round(export_slope, 4) if export_slope is not None else None,
            "current_account_pct_gdp": round(ca_balance, 2) if ca_balance is not None else None,
            "ca_trend_slope": round(ca_slope, 4) if ca_slope is not None else None,
            "competitiveness_assessment": competitiveness,
            "export_component": round(export_component, 1),
            "ca_component": round(ca_component, 1),
        }
