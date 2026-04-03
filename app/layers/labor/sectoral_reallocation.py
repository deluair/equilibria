"""Sectoral reallocation and Lilien dispersion index.

The Lilien (1982) index measures the dispersion of sectoral employment growth
rates, weighted by sector employment shares. High dispersion indicates
sectoral shifts are occurring, which cause frictional unemployment as workers
move between sectors.

Lilien dispersion index:
    sigma_t = sqrt(sum_k [share_{k,t} * (dln(E_{k,t}) - dln(E_t))^2])

where share_{k,t} is sector k's employment share and dln(E_{k,t}) is sector
k's employment growth rate.

Debate:
    Lilien (1982): sectoral shifts cause unemployment, not just aggregate demand.
    Abraham & Katz (1986) critique: Lilien index is endogenous. Aggregate demand
    shocks that differentially affect sectors will inflate the index even without
    true reallocation. Sectors with high demand sensitivity show high dispersion
    during recessions.

Adjustment (Abraham-Katz):
    Purge the Lilien index of aggregate demand effects by regressing sector
    growth on aggregate growth first, then computing dispersion of residuals.

    Step 1: dln(E_{k,t}) = a_k + b_k*dln(E_t) + u_{k,t}
    Step 2: sigma_adj_t = sqrt(sum_k [share_{k,t} * u_{k,t}^2])

Additional metrics:
    - Between-sector vs within-sector variance decomposition
    - Structural change index (sum of absolute share changes)

References:
    Lilien, D. (1982). Sectoral Shifts and Cyclical Unemployment. Journal
        of Political Economy 90(4): 777-793.
    Abraham, K. & Katz, L. (1986). Cyclical Unemployment: Sectoral Shifts
        or Aggregate Disturbances? Journal of Political Economy 94(3): 507-522.
    Autor, D. & Dorn, D. (2013). The Growth of Low-Skill Service Jobs and
        the Polarization of the US Labor Market. AER 103(5): 1553-1597.

Score: high adjusted dispersion -> STRESS (genuine reallocation friction).
Low dispersion -> STABLE.
"""

import numpy as np
from app.layers.base import LayerBase


class SectoralReallocation(LayerBase):
    layer_id = "l3"
    name = "Sectoral Reallocation (Lilien)"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'sectoral_employment'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if not rows or len(rows) < 10:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient sectoral data"}

        import json

        # Organize by date and sector
        data_by_date = {}
        sectors = set()

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            date = row["date"]
            sector = meta.get("sector", row.get("description", "unknown"))
            emp = row["value"]
            if emp is None or emp <= 0:
                continue
            sectors.add(sector)
            if date not in data_by_date:
                data_by_date[date] = {}
            data_by_date[date][sector] = float(emp)

        sorted_dates = sorted(data_by_date.keys())
        sector_list = sorted(sectors)

        if len(sorted_dates) < 3 or len(sector_list) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient periods or sectors"}

        # Build employment matrix (dates x sectors)
        n_dates = len(sorted_dates)
        n_sectors = len(sector_list)
        emp_matrix = np.zeros((n_dates, n_sectors))

        for i, date in enumerate(sorted_dates):
            for j, sector in enumerate(sector_list):
                emp_matrix[i, j] = data_by_date[date].get(sector, 0.0)

        # Compute growth rates (log differences)
        total_emp = emp_matrix.sum(axis=1)
        valid_mask = (total_emp[:-1] > 0) & (total_emp[1:] > 0)

        if np.sum(valid_mask) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient growth periods"}

        agg_growth = np.diff(np.log(total_emp))

        # Sector growth rates
        sector_growth = np.zeros((n_dates - 1, n_sectors))
        for j in range(n_sectors):
            for i in range(n_dates - 1):
                if emp_matrix[i, j] > 0 and emp_matrix[i + 1, j] > 0:
                    sector_growth[i, j] = np.log(emp_matrix[i + 1, j]) - np.log(emp_matrix[i, j])

        # Employment shares (lagged)
        shares = emp_matrix[:-1] / total_emp[:-1, None]
        shares = np.where(np.isfinite(shares), shares, 0.0)

        # Lilien dispersion index
        lilien = np.zeros(n_dates - 1)
        for t in range(n_dates - 1):
            deviations = (sector_growth[t] - agg_growth[t]) ** 2
            lilien[t] = np.sqrt(np.sum(shares[t] * deviations))

        # Abraham-Katz adjustment: purge aggregate demand effects
        # Regress each sector's growth on aggregate growth
        adjusted_resid = np.zeros_like(sector_growth)
        sector_betas = {}

        for j, sector in enumerate(sector_list):
            sg = sector_growth[:, j]
            X_ak = np.column_stack([np.ones(len(agg_growth)), agg_growth])
            beta_ak = np.linalg.lstsq(X_ak, sg, rcond=None)[0]
            adjusted_resid[:, j] = sg - X_ak @ beta_ak
            sector_betas[sector] = {
                "demand_sensitivity": round(float(beta_ak[1]), 4),
            }

        # Adjusted Lilien index
        lilien_adj = np.zeros(n_dates - 1)
        for t in range(n_dates - 1):
            lilien_adj[t] = np.sqrt(np.sum(shares[t] * adjusted_resid[t] ** 2))

        # Structural change index: sum of absolute share changes
        share_changes = np.abs(np.diff(shares, axis=0))
        structural_change = np.sum(share_changes, axis=1) if share_changes.shape[0] > 0 else np.array([0.0])

        # Current values
        current_lilien = float(lilien[-1])
        current_adj = float(lilien_adj[-1])
        mean_lilien = float(np.mean(lilien))
        mean_adj = float(np.mean(lilien_adj))

        # Correlation between Lilien and unemployment (if available)
        # For now, just report the time series

        # Score: high adjusted dispersion -> reallocation stress
        if current_adj > 0.04:
            score = 60.0 + (current_adj - 0.04) * 500.0
        elif current_adj > 0.02:
            score = 30.0 + (current_adj - 0.02) * 1500.0
        elif current_adj > 0.01:
            score = 15.0 + (current_adj - 0.01) * 1500.0
        else:
            score = current_adj * 1500.0
        score = max(0.0, min(100.0, score))

        # Most volatile sectors (highest demand sensitivity)
        sorted_sectors = sorted(sector_betas.items(), key=lambda x: abs(x[1]["demand_sensitivity"]), reverse=True)

        return {
            "score": round(score, 2),
            "country": country,
            "n_periods": n_dates,
            "n_sectors": n_sectors,
            "lilien_index": {
                "current": round(current_lilien, 4),
                "mean": round(mean_lilien, 4),
                "interpretation": (
                    "high sectoral dispersion" if current_lilien > 0.03
                    else "moderate dispersion" if current_lilien > 0.015
                    else "low dispersion"
                ),
            },
            "adjusted_index": {
                "current": round(current_adj, 4),
                "mean": round(mean_adj, 4),
                "demand_purged": True,
                "interpretation": (
                    "genuine sectoral reallocation" if current_adj > 0.02
                    else "modest reallocation" if current_adj > 0.01
                    else "low reallocation (mostly aggregate demand)"
                ),
            },
            "structural_change_index": round(float(structural_change[-1]) if len(structural_change) > 0 else 0.0, 4),
            "top_demand_sensitive_sectors": [
                {"sector": s, **v} for s, v in sorted_sectors[:5]
            ],
            "time_range": {
                "start": sorted_dates[0],
                "end": sorted_dates[-1],
            },
        }
