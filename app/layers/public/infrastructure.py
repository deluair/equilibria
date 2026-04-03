"""Infrastructure economics: public capital productivity, gaps, PPP, congestion.

Implements four core infrastructure economics models:

1. Aschauer (1989) productivity of public capital: estimates the output
   elasticity of public capital stock using aggregate production function.
       ln(Y) = a + alpha*ln(K_priv) + beta*ln(K_pub) + gamma*ln(L) + e
   Aschauer's original estimate: beta ~ 0.39 (controversial, likely upper bound).
   Modern estimates: 0.10-0.20 range. Tested with time-series and panel data.

2. Infrastructure gap estimation: compares actual infrastructure stock/quality
   to predicted levels based on income, urbanization, and geography.
   Gap = predicted_infrastructure - actual_infrastructure.
   Uses cross-country regression benchmark.

3. Public-Private Partnership (PPP) evaluation: compares lifecycle costs and
   risk allocation between public procurement and PPP.
       VFM = PV(public_costs) - PV(ppp_costs)
   Value for Money > 0 favors PPP. Accounts for risk transfer premium,
   efficiency gains, and higher private financing costs.

4. Congestion pricing (Vickrey 1969, Small & Verhoef 2007):
   Optimal toll = marginal external cost of congestion.
       toll* = n * d(travel_time)/d(n) * value_of_time
   where n is traffic volume. With BPR function:
       travel_time = free_flow * (1 + alpha * (volume/capacity)^beta)

References:
    Aschauer, D. (1989). Is Public Expenditure Productive? Journal of
        Monetary Economics, 23(2), 177-200.
    Calderlon, C. & Serven, L. (2014). Infrastructure, Growth, and
        Inequality. World Bank Policy Research WP 7034.
    Engel, E., Fischer, R. & Galetovic, A. (2014). The Economics of
        Public-Private Partnerships. Cambridge.
    Small, K. & Verhoef, E. (2007). The Economics of Urban Transportation.
        Routledge. 2nd ed.

Sources: WDI (infrastructure indicators), Penn World Table (capital stocks)
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


def _bpr_travel_time(
    volume: float,
    capacity: float,
    free_flow: float,
    alpha: float = 0.15,
    beta: float = 4.0,
) -> float:
    """Bureau of Public Roads travel time function.

    travel_time = free_flow * (1 + alpha * (volume/capacity)^beta)
    """
    ratio = volume / capacity if capacity > 0 else 0
    return free_flow * (1.0 + alpha * ratio**beta)


def _marginal_congestion_cost(
    volume: float,
    capacity: float,
    free_flow: float,
    vot: float,
    alpha: float = 0.15,
    beta: float = 4.0,
) -> float:
    """Marginal external congestion cost (optimal toll).

    d(total_delay)/d(volume) - private delay =
        volume * d(travel_time)/d(volume)
    = volume * free_flow * alpha * beta * (volume/capacity)^(beta-1) / capacity
    Multiplied by value of time.
    """
    if capacity <= 0:
        return 0.0
    ratio = volume / capacity
    marginal_delay = free_flow * alpha * beta * ratio ** (beta - 1) / capacity
    return volume * marginal_delay * vot


class InfrastructureEconomics(LayerBase):
    layer_id = "l10"
    name = "Infrastructure Economics"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")

        results = {"country": country}

        # --- Aschauer public capital productivity ---
        # Fetch output, private capital, public capital, labor
        macro_series = {
            "gdp": ("NY.GDP.MKTP.KD", "GDP_REAL"),
            "gfcf": ("NE.GDI.FTOT.ZS", "GFCF_GDP"),
            "gov_invest": ("NE.GDI.FPRV.ZS", "GOV_INVEST"),
            "labor": ("SL.TLF.TOTL.IN", "LABOR_FORCE"),
        }

        ts_data: dict[str, dict[str, float]] = {}
        for label, (wdi_code, alt_code) in macro_series.items():
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id IN (?, ?)
                  AND dp.value > 0
                ORDER BY dp.date
                """,
                (country, wdi_code, alt_code),
            )
            for r in rows:
                d = r["date"]
                ts_data.setdefault(d, {})[label] = float(r["value"])

        # Cross-country Aschauer estimation
        cross_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN (
                'NY.GDP.MKTP.KD', 'NE.GDI.FTOT.ZS',
                'GC.XPN.TOTL.GD.ZS', 'SL.TLF.TOTL.IN'
            )
              AND dp.value > 0
            ORDER BY dp.date DESC
            """
        )

        cross_data: dict[str, dict[str, float]] = {}
        for r in cross_rows:
            iso = r["country_iso3"]
            sid = r["series_id"]
            if iso not in cross_data:
                cross_data[iso] = {}
            if sid not in cross_data[iso]:
                cross_data[iso][sid] = float(r["value"])

        aschauer = {}
        usable = {
            iso: d
            for iso, d in cross_data.items()
            if all(s in d for s in ["NY.GDP.MKTP.KD", "NE.GDI.FTOT.ZS", "GC.XPN.TOTL.GD.ZS", "SL.TLF.TOTL.IN"])
        }

        if len(usable) >= 20:
            isos = sorted(usable.keys())
            n = len(isos)

            Y = np.log(np.array([usable[c]["NY.GDP.MKTP.KD"] for c in isos]))
            # Use GFCF/GDP as proxy for private capital intensity
            K_priv = np.log(np.array([max(usable[c]["NE.GDI.FTOT.ZS"], 0.1) for c in isos]))
            # Use government expenditure as proxy for public capital
            K_pub = np.log(np.array([max(usable[c]["GC.XPN.TOTL.GD.ZS"], 0.1) for c in isos]))
            L = np.log(np.array([usable[c]["SL.TLF.TOTL.IN"] for c in isos]))

            X = np.column_stack([np.ones(n), K_priv, K_pub, L])
            beta = np.linalg.lstsq(X, Y, rcond=None)[0]

            resid = Y - X @ beta
            ss_res = float(np.sum(resid**2))
            ss_tot = float(np.sum((Y - np.mean(Y)) ** 2))
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0

            # HC1 standard errors
            XtX_inv = np.linalg.inv(X.T @ X)
            omega = np.diag(resid**2) * (n / (n - 4))
            V = XtX_inv @ (X.T @ omega @ X) @ XtX_inv
            se = np.sqrt(np.maximum(np.diag(V), 0.0))

            aschauer = {
                "n_countries": n,
                "output_elasticity_private_capital": round(float(beta[1]), 4),
                "output_elasticity_public_capital": round(float(beta[2]), 4),
                "output_elasticity_labor": round(float(beta[3]), 4),
                "se_public_capital": round(float(se[2]), 4),
                "t_stat_public_capital": round(float(beta[2] / se[2]), 2) if se[2] > 0 else None,
                "r_squared": round(r2, 4),
                "returns_to_scale": round(float(beta[1] + beta[2] + beta[3]), 3),
            }
        else:
            aschauer = {"error": "insufficient cross-country data"}

        results["aschauer"] = aschauer

        # --- Infrastructure gap ---
        infra_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, ds.series_id, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id IN (
                'IS.RRS.TOTL.KM', 'IS.ROD.PAVE.ZS',
                'EG.ELC.ACCS.ZS', 'IT.NET.USER.ZS',
                'NY.GDP.PCAP.KD', 'SP.URB.TOTL.IN.ZS'
            )
              AND dp.value > 0
            ORDER BY dp.date DESC
            """
        )

        infra_data: dict[str, dict[str, float]] = {}
        for r in infra_rows:
            iso = r["country_iso3"]
            sid = r["series_id"]
            if iso not in infra_data:
                infra_data[iso] = {}
            if sid not in infra_data[iso]:
                infra_data[iso][sid] = float(r["value"])

        # Use electricity access as headline infrastructure indicator
        gap_result = {}
        usable_gap = {iso: d for iso, d in infra_data.items() if "EG.ELC.ACCS.ZS" in d and "NY.GDP.PCAP.KD" in d}

        if len(usable_gap) >= 20:
            isos_g = sorted(usable_gap.keys())
            n_g = len(isos_g)

            y_infra = np.array([usable_gap[c]["EG.ELC.ACCS.ZS"] for c in isos_g])
            x_gdppc = np.log(np.array([usable_gap[c]["NY.GDP.PCAP.KD"] for c in isos_g]))
            x_urban = np.array([usable_gap[c].get("SP.URB.TOTL.IN.ZS", 50) for c in isos_g])

            X_gap = np.column_stack([np.ones(n_g), x_gdppc, x_urban])
            b_gap = np.linalg.lstsq(X_gap, y_infra, rcond=None)[0]
            if country in usable_gap:
                c_idx = isos_g.index(country)
                predicted = float(X_gap[c_idx] @ b_gap)
                actual = y_infra[c_idx]
                gap = predicted - actual

                gap_result = {
                    "indicator": "electricity_access_pct",
                    "actual": round(actual, 1),
                    "predicted_by_income": round(predicted, 1),
                    "gap": round(gap, 1),
                    "assessment": "deficit" if gap > 5 else "surplus" if gap < -5 else "adequate",
                    "n_countries": n_g,
                }

                # Additional indicators if available
                for name, sid in [
                    ("paved_roads_pct", "IS.ROD.PAVE.ZS"),
                    ("internet_users_pct", "IT.NET.USER.ZS"),
                ]:
                    val = infra_data.get(country, {}).get(sid)
                    if val is not None:
                        gap_result[name] = round(val, 1)
            else:
                gap_result = {"error": "no infrastructure data for target country"}
        else:
            gap_result = {"error": "insufficient cross-country infrastructure data"}

        results["infrastructure_gap"] = gap_result

        # --- PPP evaluation ---
        project_cost = kwargs.get("project_cost")
        project_years = kwargs.get("project_years", 25)
        public_discount = kwargs.get("public_discount_rate", 0.05)
        private_discount = kwargs.get("private_discount_rate", 0.08)
        efficiency_gain = kwargs.get("ppp_efficiency_gain", 0.10)
        risk_transfer_value = kwargs.get("risk_transfer_pct", 0.15)

        if project_cost:
            # Public sector comparator
            annual_opex = project_cost * 0.04  # 4% of capex annually
            pv_public = project_cost + sum(annual_opex / (1 + public_discount) ** t for t in range(1, project_years + 1))

            # PPP cost: higher financing cost, but efficiency + risk transfer
            ppp_capex = project_cost * (1 - efficiency_gain)
            ppp_annual = ppp_capex * 0.04 * (1 - efficiency_gain * 0.5)
            pv_ppp_raw = ppp_capex + sum(ppp_annual / (1 + private_discount) ** t for t in range(1, project_years + 1))
            risk_value = project_cost * risk_transfer_value
            pv_ppp = pv_ppp_raw - risk_value

            vfm = pv_public - pv_ppp
            vfm_pct = (vfm / pv_public) * 100 if pv_public > 0 else 0

            results["ppp_evaluation"] = {
                "pv_public_procurement": round(pv_public, 0),
                "pv_ppp": round(pv_ppp, 0),
                "risk_transfer_value": round(risk_value, 0),
                "value_for_money": round(vfm, 0),
                "vfm_pct": round(vfm_pct, 2),
                "recommendation": "PPP preferred" if vfm > 0 else "public procurement preferred",
                "parameters": {
                    "project_cost": project_cost,
                    "project_years": project_years,
                    "public_discount_rate": public_discount,
                    "private_discount_rate": private_discount,
                    "efficiency_gain": efficiency_gain,
                    "risk_transfer_pct": risk_transfer_value,
                },
            }
        else:
            results["ppp_evaluation"] = {"note": "provide project_cost for PPP analysis"}

        # --- Congestion pricing ---
        free_flow_time = kwargs.get("free_flow_minutes", 20.0)
        road_capacity = kwargs.get("road_capacity_vehicles_hr", 2000.0)
        current_volume = kwargs.get("current_volume_vehicles_hr", 1800.0)
        value_of_time = kwargs.get("value_of_time_per_hour", 25.0)

        current_time = _bpr_travel_time(current_volume, road_capacity, free_flow_time)
        optimal_toll = _marginal_congestion_cost(current_volume, road_capacity, free_flow_time, value_of_time / 60.0)

        # Volume-toll curve
        volumes = np.linspace(0.1 * road_capacity, 1.5 * road_capacity, 30)
        times = [_bpr_travel_time(v, road_capacity, free_flow_time) for v in volumes]
        tolls = [_marginal_congestion_cost(v, road_capacity, free_flow_time, value_of_time / 60.0) for v in volumes]

        delay_cost = (current_time - free_flow_time) * (value_of_time / 60.0) * current_volume
        vol_cap_ratio = current_volume / road_capacity

        results["congestion_pricing"] = {
            "free_flow_time_min": free_flow_time,
            "current_travel_time_min": round(current_time, 1),
            "delay_min": round(current_time - free_flow_time, 1),
            "volume_capacity_ratio": round(vol_cap_ratio, 3),
            "optimal_toll": round(optimal_toll, 2),
            "total_delay_cost_per_hour": round(delay_cost, 0),
            "congestion_level": (
                "severe" if vol_cap_ratio > 1.0 else "heavy" if vol_cap_ratio > 0.85 else "moderate" if vol_cap_ratio > 0.7 else "light"
            ),
            "curve": {
                "volumes": [round(float(v), 0) for v in volumes[::3]],
                "travel_times": [round(float(t), 1) for t in times[::3]],
                "optimal_tolls": [round(float(t), 2) for t in tolls[::3]],
            },
        }

        # --- Score ---
        score = 25.0

        # Aschauer: low public capital productivity
        pub_elast = aschauer.get("output_elasticity_public_capital")
        if pub_elast is not None:
            if pub_elast < 0.05:
                score += 20  # ineffective public investment
            elif pub_elast > 0.30:
                score += 10  # suspiciously high (possible endogeneity)

        # Infrastructure gap
        gap_val = gap_result.get("gap")
        if gap_val is not None:
            if gap_val > 20:
                score += 25
            elif gap_val > 10:
                score += 15
            elif gap_val > 5:
                score += 5

        # Congestion
        if vol_cap_ratio > 1.0:
            score += 15
        elif vol_cap_ratio > 0.85:
            score += 8

        score = max(0.0, min(100.0, score))

        return {"score": round(score, 1), "results": results}
