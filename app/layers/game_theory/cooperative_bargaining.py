"""Cooperative Bargaining module.

Proxies Nash (1950) bargaining power via labor share of income.
Declining labor share signals erosion of workers' bargaining position
relative to capital (Karabarbounis & Neiman 2014).

Primary series: SL.GDP.PCAP.EM.KD (GDP per person employed, constant USD).
Stress = rate of decline relative to overall GDP per capita growth.

Sources: WDI (SL.GDP.PCAP.EM.KD, NY.GDP.PCAP.KD)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class CooperativeBargaining(LayerBase):
    layer_id = "lGT"
    name = "Cooperative Bargaining"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        labor_prod_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'SL.GDP.PCAP.EM.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        gdp_pc_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NY.GDP.PCAP.KD'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not labor_prod_rows or len(labor_prod_rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data: need SL.GDP.PCAP.EM.KD (min 5 obs)",
            }

        labor_vals = np.array([float(r["value"]) for r in labor_prod_rows])
        dates = [r["date"] for r in labor_prod_rows]

        # Compute trend in labor productivity via OLS slope
        t = np.arange(len(labor_vals), dtype=float)
        labor_slope = float(np.polyfit(t, labor_vals, 1)[0])
        labor_mean = float(np.mean(labor_vals))
        labor_growth_rate = labor_slope / max(abs(labor_mean), 1e-10)

        # If GDP per capita data available, compute gap between labor and GDP trends
        if gdp_pc_rows and len(gdp_pc_rows) >= 5:
            # Align by date
            labor_dates = {r["date"]: float(r["value"]) for r in labor_prod_rows}
            gdp_dates = {r["date"]: float(r["value"]) for r in gdp_pc_rows}
            common = sorted(set(labor_dates) & set(gdp_dates))

            if len(common) >= 5:
                lv = np.array([labor_dates[d] for d in common])
                gv = np.array([gdp_dates[d] for d in common])
                t_c = np.arange(len(common), dtype=float)

                lslope = float(np.polyfit(t_c, lv, 1)[0])
                gslope = float(np.polyfit(t_c, gv, 1)[0])

                # Labor productivity growing slower than GDP = bargaining power loss
                divergence = (gslope - lslope) / max(abs(gslope), 1e-10)
                bargaining_loss = float(np.clip(divergence * 100.0, 0.0, 100.0))
            else:
                bargaining_loss = float(np.clip(-labor_growth_rate * 500.0, 0.0, 100.0))
        else:
            # Fallback: negative labor productivity trend = stress
            bargaining_loss = float(np.clip(-labor_growth_rate * 500.0, 0.0, 100.0))

        score = float(np.clip(bargaining_loss, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "labor_productivity_trend_slope": round(labor_slope, 4),
            "labor_productivity_mean": round(labor_mean, 2),
            "labor_growth_rate": round(labor_growth_rate, 6),
            "n_labor_obs": len(labor_prod_rows),
            "n_gdp_obs": len(gdp_pc_rows) if gdp_pc_rows else 0,
            "period": f"{dates[0]} to {dates[-1]}",
            "interpretation": (
                "severe bargaining power erosion" if score > 60
                else "moderate bargaining power loss" if score > 30
                else "stable bargaining equilibrium"
            ),
        }
