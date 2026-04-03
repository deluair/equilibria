"""Energy security analysis: import dependence, diversification, disruption risk, energy poverty.

Methodology
-----------
**Import dependence index** (IEA definition):
    IDI = net_energy_imports / total_primary_energy_supply
    Ranges from 0 (self-sufficient) to 1 (fully dependent). Values > 0.5
    indicate high vulnerability to supply disruptions.

**Diversification (Shannon-Wiener index)**:
    H = -sum(p_i * ln(p_i))
    where p_i is the share of energy source i (or import origin i) in the
    total supply. Higher H = more diversified = more secure.

    Normalized: H_norm = H / ln(N), where N = number of sources.
    Maximum diversification = 1.0, complete concentration = 0.0.

    Applied to both fuel mix and supplier diversification.

**Supply disruption probability**:
    Estimated via historical frequency of disruption events (>5% supply drop)
    combined with geopolitical risk scores of major suppliers. Uses a simple
    Poisson process: P(disruption in next year) = 1 - exp(-lambda), where
    lambda is the historical arrival rate weighted by current supplier risk.

**Energy poverty measurement** (IEA/WHO):
    - Electrification rate (% population with electricity access)
    - Clean cooking access (% population using clean fuels)
    - Energy affordability ratio: household energy expenditure / income
    - Multidimensional Energy Poverty Index (MEPI): composite of access,
      clean cooking, reliability, affordability, and consumption adequacy.

Score reflects overall energy insecurity: high import dependence with low
diversification and high poverty raises the score.

Sources: EIA, IEA, World Bank WDI
"""

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class EnergySecurity(LayerBase):
    layer_id = "l16"
    name = "Energy Security"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")

        series_map = {
            "energy_imports": f"ENERGY_IMPORTS_{country}",
            "energy_exports": f"ENERGY_EXPORTS_{country}",
            "tpes": f"TPES_{country}",
            "oil_share": f"OIL_SHARE_{country}",
            "gas_share": f"GAS_SHARE_{country}",
            "coal_share": f"COAL_SHARE_{country}",
            "nuclear_share": f"NUCLEAR_SHARE_{country}",
            "hydro_share": f"HYDRO_SHARE_{country}",
            "renewables_share": f"RENEWABLES_SHARE_{country}",
            "electrification": f"ELECTRIFICATION_{country}",
            "clean_cooking": f"CLEAN_COOKING_{country}",
            "energy_expenditure_ratio": f"ENERGY_EXPENDITURE_RATIO_{country}",
            "supply_history": f"ENERGY_SUPPLY_HISTORY_{country}",
            "supplier_risk": f"SUPPLIER_GEOPOLITICAL_RISK_{country}",
        }
        data = {}
        for label, code in series_map.items():
            rows = await db.execute_fetchall(
                "SELECT date, value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE code = ?) ORDER BY date",
                (code,),
            )
            if rows:
                data[label] = {r[0]: float(r[1]) for r in rows}

        results = {"country": country}

        # --- Import Dependence Index ---
        if "energy_imports" in data and "tpes" in data:
            common = sorted(set(data["energy_imports"]) & set(data["tpes"]))
            exports = data.get("energy_exports", {})
            if exports:
                common = sorted(set(common) & set(exports))

            if common:
                idi_series = []
                for d in common:
                    imports = data["energy_imports"][d]
                    exp = exports.get(d, 0)
                    tpes = data["tpes"][d]
                    idi = (imports - exp) / tpes if tpes > 0 else 0
                    idi_series.append((d, float(np.clip(idi, 0, 1))))

                latest_idi = idi_series[-1][1]
                idi_values = np.array([v for _, v in idi_series])

                # Trend via OLS
                t_arr = np.arange(len(idi_values), dtype=float)
                if len(t_arr) >= 5:
                    slope, intercept, r, p, se = sp_stats.linregress(t_arr, idi_values)
                    trend = "increasing" if slope > 0 and p < 0.10 else (
                        "decreasing" if slope < 0 and p < 0.10 else "stable"
                    )
                else:
                    slope, p, trend = 0, 1, "insufficient data"

                results["import_dependence"] = {
                    "latest": round(latest_idi, 3),
                    "latest_date": idi_series[-1][0],
                    "mean": round(float(np.mean(idi_values)), 3),
                    "trend": trend,
                    "trend_slope": round(float(slope), 5),
                    "trend_pvalue": round(float(p), 4),
                    "high_dependence": latest_idi > 0.5,
                    "n_obs": len(idi_series),
                }

        # --- Shannon-Wiener diversification (fuel mix) ---
        share_keys = ["oil_share", "gas_share", "coal_share", "nuclear_share",
                       "hydro_share", "renewables_share"]
        available_shares = {k: data[k] for k in share_keys if k in data}

        if len(available_shares) >= 3:
            # Use latest common date
            all_dates = [set(v.keys()) for v in available_shares.values()]
            common_dates = sorted(set.intersection(*all_dates))

            if common_dates:
                latest_d = common_dates[-1]
                shares = np.array([available_shares[k][latest_d] for k in available_shares])
                shares = shares / shares.sum() if shares.sum() > 0 else shares

                # Remove zero shares for log
                nonzero = shares[shares > 0]
                shannon = -float(np.sum(nonzero * np.log(nonzero)))
                max_shannon = np.log(len(nonzero)) if len(nonzero) > 1 else 1.0
                normalized = shannon / max_shannon if max_shannon > 0 else 0.0

                results["fuel_mix_diversification"] = {
                    "shannon_wiener": round(shannon, 3),
                    "normalized": round(float(normalized), 3),
                    "shares": {k.replace("_share", ""): round(float(available_shares[k][latest_d]), 3)
                               for k in available_shares},
                    "n_sources": len(nonzero),
                    "well_diversified": float(normalized) > 0.6,
                    "date": latest_d,
                }

        # --- Supply disruption probability ---
        if "supply_history" in data:
            supply_vals = np.array([data["supply_history"][d]
                                    for d in sorted(data["supply_history"])])
            if len(supply_vals) > 12:
                # Detect disruptions: >5% month-over-month decline
                pct_change = np.diff(supply_vals) / supply_vals[:-1]
                disruptions = np.sum(pct_change < -0.05)
                n_periods = len(pct_change)
                lambda_rate = disruptions / n_periods if n_periods > 0 else 0

                # Adjust by geopolitical risk if available
                geo_risk = 1.0
                if "supplier_risk" in data:
                    risk_vals = list(data["supplier_risk"].values())
                    geo_risk = float(np.mean(risk_vals[-4:])) if risk_vals else 1.0

                adjusted_lambda = lambda_rate * geo_risk * 12  # annualize
                prob_disruption = 1 - np.exp(-adjusted_lambda)

                results["disruption_probability"] = {
                    "annual_probability": round(float(prob_disruption), 3),
                    "historical_rate": round(float(lambda_rate), 4),
                    "n_disruptions": int(disruptions),
                    "n_periods": int(n_periods),
                    "geopolitical_adjustment": round(float(geo_risk), 2),
                    "high_risk": float(prob_disruption) > 0.15,
                }

        # --- Energy poverty (MEPI) ---
        poverty_metrics = {}
        if "electrification" in data:
            vals = list(data["electrification"].values())
            poverty_metrics["electrification_rate"] = round(float(vals[-1]), 1)
        if "clean_cooking" in data:
            vals = list(data["clean_cooking"].values())
            poverty_metrics["clean_cooking_access"] = round(float(vals[-1]), 1)
        if "energy_expenditure_ratio" in data:
            vals = list(data["energy_expenditure_ratio"].values())
            poverty_metrics["affordability_ratio"] = round(float(vals[-1]), 3)

        if poverty_metrics:
            # MEPI composite: equal-weighted deprivation score
            deprivation_scores = []
            if "electrification_rate" in poverty_metrics:
                deprivation_scores.append(1 - poverty_metrics["electrification_rate"] / 100)
            if "clean_cooking_access" in poverty_metrics:
                deprivation_scores.append(1 - poverty_metrics["clean_cooking_access"] / 100)
            if "affordability_ratio" in poverty_metrics:
                # >10% of income on energy = deprived
                deprivation_scores.append(min(poverty_metrics["affordability_ratio"] / 0.10, 1.0))

            mepi = float(np.mean(deprivation_scores)) if deprivation_scores else 0
            poverty_metrics["mepi"] = round(mepi, 3)
            poverty_metrics["energy_poor"] = mepi > 0.3

            results["energy_poverty"] = poverty_metrics

        # --- Score ---
        score = 15.0  # baseline

        # Import dependence
        idi_info = results.get("import_dependence", {})
        if idi_info:
            idi = idi_info.get("latest", 0)
            score += min(idi * 30, 25)
            if idi_info.get("trend") == "increasing":
                score += 5

        # Diversification penalty
        div_info = results.get("fuel_mix_diversification", {})
        if div_info:
            norm = div_info.get("normalized", 0.5)
            score += max((1 - norm) * 20, 0)

        # Disruption risk
        disr_info = results.get("disruption_probability", {})
        if disr_info:
            prob = disr_info.get("annual_probability", 0)
            score += min(prob * 50, 20)

        # Energy poverty
        pov_info = results.get("energy_poverty", {})
        if pov_info:
            mepi_val = pov_info.get("mepi", 0)
            score += min(mepi_val * 25, 15)

        score = float(np.clip(score, 0, 100))

        return {"score": round(score, 1), "results": results}
