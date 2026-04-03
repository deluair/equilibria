"""Active aging index: elderly labor participation proxy.

Active aging refers to continued participation in economic, social, and
civic life beyond traditional retirement age. High labor force participation
combined with a moderate elderly share indicates productive aging and
reduced fiscal dependency. Low participation with high elderly share
signals economic exclusion or structural retirement-push.

Score: high elderly share + low LFPR -> STRESS (inactive aging burden),
moderate elderly share + high LFPR -> STABLE (active aging dividend).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ActiveAgingIndex(LayerBase):
    layer_id = "lAG"
    name = "Active Aging Index"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        pop_code = "SP.POP.65UP.TO.ZS"
        lfpr_code = "SL.TLF.CACT.ZS"

        pop_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pop_code, "%Population ages 65%"),
        )
        lfpr_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (lfpr_code, "%labor force participation%"),
        )

        pop_vals = [r["value"] for r in pop_rows if r["value"] is not None]
        lfpr_vals = [r["value"] for r in lfpr_rows if r["value"] is not None]

        if not pop_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for elderly population share SP.POP.65UP.TO.ZS",
            }

        elderly_share = pop_vals[0]
        lfpr = lfpr_vals[0] if lfpr_vals else None

        # Base from elderly share (higher share = more aging pressure)
        if elderly_share < 7:
            base = 15.0
        elif elderly_share < 14:
            base = 20.0 + (elderly_share - 7) * 3.0
        elif elderly_share < 21:
            base = 41.0 + (elderly_share - 14) * 3.0
        else:
            base = min(85.0, 62.0 + (elderly_share - 21) * 2.0)

        # Labor force participation moderates score
        # High LFPR = active aging -> reduce stress
        # Low LFPR = inactive aging -> increase stress
        if lfpr is not None:
            if lfpr > 70:
                base = max(5.0, base - 15.0)
            elif lfpr > 60:
                base = max(5.0, base - 8.0)
            elif lfpr < 50:
                base = min(100.0, base + 10.0)
            elif lfpr < 40:
                base = min(100.0, base + 18.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "elderly_share_pct": round(elderly_share, 2),
                "labor_force_participation_rate": round(lfpr, 2) if lfpr is not None else None,
                "n_obs_pop": len(pop_vals),
                "n_obs_lfpr": len(lfpr_vals),
                "active_aging": lfpr is not None and lfpr > 60,
            },
        }
