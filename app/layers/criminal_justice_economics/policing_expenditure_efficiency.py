"""Policing expenditure efficiency: public safety spending vs crime reduction outcomes.

Public order and safety expenditure as a share of GDP measures the resource commitment
to law enforcement. Efficiency is assessed by whether higher spending correlates with
lower crime rates. Excessive spending without crime reduction signals poor returns or
structural crime problems. Optimal policing budgets typically range 1-3% of GDP.

Score: efficient spending (low crime, moderate budget) -> STABLE, rising budgets
with flat crime -> WATCH, high spending with persistent crime -> STRESS,
runaway security budgets -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class PolicingExpenditureEfficiency(LayerBase):
    layer_id = "lCJ"
    name = "Policing Expenditure Efficiency"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # WDI: public order and safety expenditure % of GDP
        pol_code = "GC.XPN.COMP.ZS"
        pol_name = "compensation of employees"

        hom_code = "VC.IHR.PSRC.P5"
        hom_name = "homicide"

        pol_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (pol_code, f"%{pol_name}%"),
        )
        hom_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (hom_code, f"%{hom_name}%"),
        )

        pol_vals = [r["value"] for r in pol_rows if r["value"] is not None]
        hom_vals = [r["value"] for r in hom_rows if r["value"] is not None]

        if not pol_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for policing expenditure proxy GC.XPN.COMP.ZS",
            }

        spending = pol_vals[0]
        homicide = hom_vals[0] if hom_vals else None

        # Base score from spending level
        if spending < 20:
            base = 15.0 + spending * 0.5
        elif spending < 35:
            base = 25.0 + (spending - 20) * 1.5
        elif spending < 50:
            base = 47.5 + (spending - 35) * 1.5
        else:
            base = min(95.0, 70.0 + (spending - 50) * 1.0)

        # Augment: if high crime persists despite spending, add inefficiency penalty
        if homicide is not None and homicide > 10:
            base = min(100.0, base + 10.0)
        elif homicide is not None and homicide > 5:
            base = min(100.0, base + 5.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "govt_compensation_pct": round(spending, 2),
                "homicide_rate_per_100k": round(homicide, 2) if homicide is not None else None,
                "n_obs_spending": len(pol_vals),
                "n_obs_crime": len(hom_vals),
                "efficiency_flag": homicide is not None and homicide > 10 and spending > 35,
            },
        }
