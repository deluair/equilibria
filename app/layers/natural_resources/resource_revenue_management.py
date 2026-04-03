"""Resource revenue management: quality of resource revenue governance.

Proxies governance quality for resource revenues using composite WDI/WGI indicators:
  CC.EST   - control of corruption (World Bank WGI, -2.5 to +2.5)
  RL.EST   - rule of law (World Bank WGI, -2.5 to +2.5)
  GE.EST   - government effectiveness (World Bank WGI, -2.5 to +2.5)
  NY.GDP.TOTL.RT.ZS - resource rents (% GDP) to weight governance relevance

Revenue management score (higher = poorer governance of resource revenues):
  governance_avg = mean(CC, RL, GE) in [-2.5, +2.5]
  governance_score = clip((2.5 - governance_avg) / 5.0 * 70, 0, 70)
  rent_weight = clip(rent_pct * 0.6, 0, 30)
  score = clip(governance_score + rent_weight, 0, 100)

Sources: World Bank WGI (governance) + WDI (rents)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_WGI_SERIES = ["CC.EST", "RL.EST", "GE.EST"]


class ResourceRevenueManagement(LayerBase):
    layer_id = "lNR"
    name = "Resource Revenue Management"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN (
                'CC.EST', 'RL.EST', 'GE.EST', 'NY.GDP.TOTL.RT.ZS'
            )
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 40
            """,
            (country,),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no governance or rent data",
            }

        latest: dict[str, tuple[str, float]] = {}
        for r in rows:
            sid = r["series_id"]
            if sid not in latest and r["value"] is not None:
                latest[sid] = (r["date"][:4], float(r["value"]))

        wgi_vals = [latest[s][1] for s in _WGI_SERIES if s in latest]
        rent_data = latest.get("NY.GDP.TOTL.RT.ZS")

        if not wgi_vals and rent_data is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no governance or rent indicators available",
            }

        s_governance = 0.0
        governance_avg = None
        if wgi_vals:
            governance_avg = float(np.mean(wgi_vals))
            s_governance = float(np.clip((2.5 - governance_avg) / 5.0 * 70.0, 0, 70))

        s_rent = 0.0
        rent_pct = None
        if rent_data:
            rent_pct = rent_data[1]
            s_rent = float(np.clip(rent_pct * 0.6, 0, 30))

        score = float(np.clip(s_governance + s_rent, 0, 100))

        governance_quality = (
            "poor" if (governance_avg or 0) < -1.0
            else "weak" if (governance_avg or 0) < 0.0
            else "moderate" if (governance_avg or 0) < 0.75
            else "good"
        )

        return {
            "score": round(score, 2),
            "results": {
                "country_iso3": country,
                "governance_indicators": {
                    s: round(latest[s][1], 4) for s in _WGI_SERIES if s in latest
                },
                "governance_avg_wgi": (
                    round(governance_avg, 4) if governance_avg is not None else None
                ),
                "resource_rent_pct_gdp": round(rent_pct, 3) if rent_pct is not None else None,
                "governance_quality": governance_quality,
                "sub_scores": {
                    "governance": round(s_governance, 2),
                    "rent_relevance": round(s_rent, 2),
                },
                "n_wgi_indicators": len(wgi_vals),
            },
        }
