"""Just transition risk: SL.IND.EMPL.ZS + EG.ELC.RNEW.ZS.

Methodology
-----------
A just transition ensures that the shift away from fossil fuels does not leave
behind workers and communities dependent on carbon-intensive industries. Just
transition risk is highest when:
- A large share of the workforce is in industry (including fossil fuel extraction
  and energy-intensive manufacturing): SL.IND.EMPL.ZS
- Renewable energy penetration is rising rapidly, implying accelerating structural
  disruption to carbon-intensive sectors

The risk is not that renewables are growing (which is good for climate) but that
the speed of transition may outpace the ability of industrial workers to relocate
or retrain. Countries with high industrial employment shares AND fast renewable
growth face the greatest just transition challenge.

Combined risk score:
- Industrial employment > 30% AND renewable growth > 3 pp/yr: high risk
- Low industrial employment or slow renewable growth: low risk

Score: 0 = low just transition risk (low industrial employment or slow transition),
100 = high risk (high industrial employment + rapid transition pace).

Sources: World Bank WDI SL.IND.EMPL.ZS (employment in industry, % total),
EG.ELC.RNEW.ZS (renewable electricity output, % total).
ILO Just Transition policy brief (2015). IRENA Renewable Energy and Jobs 2023.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase

_IND_CODE = "SL.IND.EMPL.ZS"
_IND_NAME = "Employment in industry"
_REN_CODE = "EG.ELC.RNEW.ZS"
_REN_NAME = "Renewable electricity output"


class JustTransitionRisk(LayerBase):
    layer_id = "lGT"
    name = "Just Transition Risk"

    IND_EMPL_HIGH = 30.0   # % industry employment considered high
    REN_GROWTH_FAST = 3.0  # pp/yr renewable growth considered rapid

    async def compute(self, db, **kwargs) -> dict:
        ind_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 5",
            (_IND_CODE, f"%{_IND_NAME}%"),
        )
        ren_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (_REN_CODE, f"%{_REN_NAME}%"),
        )

        if not ind_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no industrial employment data (SL.IND.EMPL.ZS)"}

        ind_vals = [float(r["value"]) for r in ind_rows if r["value"] is not None]
        if not ind_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid industrial employment values"}

        latest_ind = ind_vals[0]

        # Industrial employment sub-score (0-60): higher employment = higher risk
        if latest_ind >= self.IND_EMPL_HIGH:
            ind_score = 60.0
        else:
            ind_score = (latest_ind / self.IND_EMPL_HIGH) * 60

        # Renewable growth sub-score (0-40): faster transition = more disruption risk
        ren_vals = [float(r["value"]) for r in ren_rows if r["value"] is not None] if ren_rows else []
        if len(ren_vals) >= 3:
            arr = np.array(ren_vals, dtype=float)
            t = np.arange(len(arr), dtype=float)
            ren_growth = float(np.polyfit(t[::-1], arr, 1)[0])
        else:
            ren_growth = 0.0

        if ren_growth >= self.REN_GROWTH_FAST:
            ren_score = 40.0
        elif ren_growth > 0:
            ren_score = (ren_growth / self.REN_GROWTH_FAST) * 40
        else:
            ren_score = 0.0  # no transition = no disruption risk

        score = min(ind_score + ren_score, 100.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "industry_employment_pct": round(latest_ind, 2),
                "renewable_growth_pp_yr": round(ren_growth, 3) if ren_vals else None,
                "ind_empl_high_threshold_pct": self.IND_EMPL_HIGH,
                "ren_growth_fast_threshold_pp_yr": self.REN_GROWTH_FAST,
            },
        }
