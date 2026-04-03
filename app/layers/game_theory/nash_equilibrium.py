"""Nash Equilibrium Analysis module.

Proxies market structure stability via the volatility of industry vs services
sector composition (Nash 1950, Dixit & Stiglitz 1977).

In a Nash equilibrium, sector shares stabilize. High year-over-year volatility
in industry/services ratio signals that agents have not settled into consistent
strategies, indicating market instability or non-equilibrium dynamics.

Score = normalized coefficient of variation of (industry_share / services_share)
ratio over time, clipped to [0, 100].

Sources: WDI (NV.IND.TOTL.ZS, NV.SRV.TOTL.ZS)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class NashEquilibriumAnalysis(LayerBase):
    layer_id = "lGT"
    name = "Nash Equilibrium Analysis"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        ind_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.IND.TOTL.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        srv_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id = 'NV.SRV.TOTL.ZS'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not ind_rows or not srv_rows or len(ind_rows) < 5 or len(srv_rows) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data: need NV.IND.TOTL.ZS and NV.SRV.TOTL.ZS (min 5 obs)",
            }

        ind_map = {r["date"]: float(r["value"]) for r in ind_rows}
        srv_map = {r["date"]: float(r["value"]) for r in srv_rows}
        common = sorted(set(ind_map) & set(srv_map))

        if len(common) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "fewer than 5 overlapping dates between industry and services series",
            }

        ratios = []
        for d in common:
            sv = srv_map[d]
            if abs(sv) > 1e-10:
                ratios.append(ind_map[d] / sv)

        if len(ratios) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient valid ratio observations",
            }

        ratios_arr = np.array(ratios)
        mean_ratio = float(np.mean(ratios_arr))
        std_ratio = float(np.std(ratios_arr))
        cv = std_ratio / max(abs(mean_ratio), 1e-10)

        # YoY changes in ratio
        changes = np.diff(ratios_arr)
        volatility = float(np.std(changes)) if len(changes) > 1 else 0.0

        # Score: CV scaled to 0-100 (CV of 0.5 = score 100)
        cv_score = float(np.clip(cv / 0.5 * 100.0, 0.0, 100.0))
        vol_score = float(np.clip(volatility / 0.5 * 50.0, 0.0, 50.0))
        score = float(np.clip((cv_score + vol_score) / 2.0, 0.0, 100.0))

        return {
            "score": round(score, 1),
            "country": country,
            "industry_to_services_ratio_mean": round(mean_ratio, 4),
            "ratio_std": round(std_ratio, 4),
            "coefficient_of_variation": round(cv, 4),
            "yoy_volatility": round(volatility, 4),
            "n_obs": len(ratios),
            "period": f"{common[0]} to {common[-1]}",
            "interpretation": (
                "high market instability: sector shares not at Nash equilibrium"
                if score > 60
                else "moderate sector volatility" if score > 30
                else "stable sector equilibrium"
            ),
        }
