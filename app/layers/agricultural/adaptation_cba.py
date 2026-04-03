"""Cost-benefit analysis of agricultural adaptation measures.

Evaluates the economic returns to agricultural adaptation investments
including irrigation infrastructure, drought-resistant crop varieties,
soil conservation, and climate-smart agriculture practices.

Methodology:
    For each adaptation measure, computes:
    - Net Present Value (NPV): sum of discounted net benefits over project life
        NPV = sum_{t=0}^{T} (B_t - C_t) / (1 + r)^t
    - Benefit-Cost Ratio (BCR): PV(benefits) / PV(costs)
    - Internal Rate of Return (IRR): discount rate where NPV = 0
    - Payback period: years until cumulative net benefits turn positive

    Benefits include yield gains, reduced yield variance (risk premium),
    reduced post-harvest losses, and avoided disaster damages.
    Costs include capital investment, operating costs, and opportunity costs.

    The composite score reflects average BCR across measures, where
    BCR < 1 indicates maladaptation (high stress score).

References:
    Mendelsohn, R. & Dinar, A. (2009). "Climate Change and Agriculture:
        An Economic Analysis of Global Impacts, Adaptation and
        Distributional Effects." Edward Elgar.
    World Bank (2010). "Economics of Adaptation to Climate Change."
    Hallegatte, S. et al. (2016). "Shock Waves: Managing the Impacts of
        Climate Change on Poverty." World Bank.
"""

from __future__ import annotations

import numpy as np
from scipy import optimize

from app.layers.base import LayerBase


class AdaptationCBA(LayerBase):
    layer_id = "l5"
    name = "Adaptation Cost-Benefit Analysis"

    # Default discount rate for agricultural investments
    DEFAULT_DISCOUNT_RATE = 0.08
    DEFAULT_PROJECT_LIFE = 25

    async def compute(self, db, **kwargs) -> dict:
        """Compute CBA for agricultural adaptation measures.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            discount_rate : float - annual discount rate (default 0.08)
            project_life : int - years for NPV computation (default 25)
        """
        country = kwargs.get("country_iso3", "BGD")
        discount_rate = kwargs.get("discount_rate", self.DEFAULT_DISCOUNT_RATE)
        project_life = kwargs.get("project_life", self.DEFAULT_PROJECT_LIFE)

        rows = await db.fetch_all(
            """
            SELECT dp.value, ds.description, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'adaptation_measures'
              AND ds.country_iso3 = ?
            ORDER BY ds.description, dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient adaptation measure data"}

        import json

        # Group by adaptation measure
        measures: dict[str, list[dict]] = {}
        for row in rows:
            desc = row["description"] or "unknown"
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            measures.setdefault(desc, []).append({
                "value": float(row["value"]),
                **meta,
            })

        results = []
        for measure_name, data_points in measures.items():
            # Extract cost and benefit streams
            capital_cost = sum(d.get("capital_cost", 0) for d in data_points)
            annual_op_cost = np.mean([d.get("operating_cost", 0) for d in data_points])
            annual_benefit = np.mean([d.get("annual_benefit", d["value"]) for d in data_points])
            yield_gain_pct = np.mean([d.get("yield_gain_pct", 0) for d in data_points])
            risk_reduction_pct = np.mean([d.get("risk_reduction_pct", 0) for d in data_points])

            if capital_cost <= 0 and annual_op_cost <= 0:
                continue

            # Build cash flow stream
            costs = np.zeros(project_life + 1)
            benefits = np.zeros(project_life + 1)
            costs[0] = capital_cost
            costs[1:] = annual_op_cost
            benefits[1:] = annual_benefit

            # Risk premium benefit: reduced variance has economic value
            # Following Hallegatte et al., risk premium = 0.5 * CRRA * variance_reduction
            crra = 2.0  # coefficient of relative risk aversion
            baseline_cv = np.mean([d.get("yield_cv", 0.2) for d in data_points])
            adapted_cv = baseline_cv * (1 - risk_reduction_pct / 100.0)
            risk_premium = 0.5 * crra * (baseline_cv ** 2 - adapted_cv ** 2) * annual_benefit
            benefits[1:] += risk_premium

            net_flow = benefits - costs

            # NPV
            npv = self._compute_npv(net_flow, discount_rate)

            # BCR
            pv_benefits = self._compute_npv(benefits, discount_rate)
            pv_costs = self._compute_npv(costs, discount_rate)
            bcr = pv_benefits / pv_costs if pv_costs > 0 else float("inf")

            # IRR
            irr = self._compute_irr(net_flow)

            # Payback period
            cumulative = np.cumsum(net_flow)
            payback_years = None
            for yr, cum in enumerate(cumulative):
                if cum >= 0 and yr > 0:
                    payback_years = yr
                    break

            # Sensitivity: NPV at +/- 2pp discount rate
            npv_low_r = self._compute_npv(net_flow, discount_rate - 0.02)
            npv_high_r = self._compute_npv(net_flow, discount_rate + 0.02)

            results.append({
                "measure": measure_name,
                "capital_cost": round(capital_cost, 2),
                "annual_operating_cost": round(float(annual_op_cost), 2),
                "annual_benefit": round(float(annual_benefit), 2),
                "yield_gain_pct": round(float(yield_gain_pct), 2),
                "risk_reduction_pct": round(float(risk_reduction_pct), 2),
                "risk_premium_annual": round(float(risk_premium), 2),
                "npv": round(float(npv), 2),
                "bcr": round(float(bcr), 3),
                "irr": round(float(irr), 4) if irr is not None else None,
                "payback_years": payback_years,
                "sensitivity": {
                    "npv_at_low_discount": round(float(npv_low_r), 2),
                    "npv_at_high_discount": round(float(npv_high_r), 2),
                },
                "n_observations": len(data_points),
            })

        if not results:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "no valid adaptation measures found"}

        # Aggregate score: average BCR mapped to 0-100
        avg_bcr = float(np.mean([r["bcr"] for r in results if np.isfinite(r["bcr"])]))
        n_positive_npv = sum(1 for r in results if r["npv"] > 0)
        n_total = len(results)

        # Score: BCR < 1 is bad (high score), BCR > 3 is excellent (low score)
        # Map BCR to score: BCR=0 -> 100, BCR=1 -> 60, BCR=2 -> 30, BCR>=3 -> 0
        if avg_bcr >= 3.0:
            score = 0.0
        elif avg_bcr >= 1.0:
            score = 60.0 - (avg_bcr - 1.0) * 30.0
        else:
            score = 60.0 + (1.0 - avg_bcr) * 40.0
        score = float(np.clip(score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "discount_rate": discount_rate,
            "project_life_years": project_life,
            "n_measures": n_total,
            "n_positive_npv": n_positive_npv,
            "average_bcr": round(avg_bcr, 3),
            "measures": results,
        }

    @staticmethod
    def _compute_npv(cash_flows: np.ndarray, rate: float) -> float:
        """Net present value of a cash flow stream."""
        t = np.arange(len(cash_flows))
        discount_factors = (1 + rate) ** (-t)
        return float(np.sum(cash_flows * discount_factors))

    @staticmethod
    def _compute_irr(cash_flows: np.ndarray) -> float | None:
        """Internal rate of return via root finding on NPV equation."""
        def npv_func(r):
            t = np.arange(len(cash_flows))
            return float(np.sum(cash_flows / (1 + r) ** t))

        try:
            result = optimize.brentq(npv_func, -0.5, 5.0, xtol=1e-6, maxiter=200)
            return float(result)
        except (ValueError, RuntimeError):
            return None
