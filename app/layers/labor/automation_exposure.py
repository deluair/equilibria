"""Automation and AI exposure indices by occupation.

Constructs occupational risk scores using three complementary frameworks:

1. Frey-Osborne (2017) automation probability:
    ML classifier trained on O*NET task descriptions to predict automation
    susceptibility. Bottleneck variables: perception/manipulation, creative
    intelligence, social intelligence.
    Original finding: 47% of US employment at high risk.

2. Routine Task Intensity (RTI) index (Autor, Levy & Murnane 2003):
    RTI = ln(routine) - ln(manual) - ln(abstract)
    High RTI occupations (bookkeeping, assembly) are most automatable.
    Drives job polarization: middle-skill routine jobs hollowed out.

3. Webb (2020) AI exposure:
    Patent-to-task mapping using NLP on patent text and O*NET task descriptions.
    Distinct from robotics exposure: AI targets high-skill cognitive tasks
    (radiology, legal research), not just routine manual.

Aggregate risk:
    Employment-weighted mean of occupation scores gives country-level
    automation exposure. Structural transformation path matters: countries
    with large routine-task employment face bigger adjustments.

References:
    Frey, C.B. & Osborne, M. (2017). The Future of Employment: How
        Susceptible Are Jobs to Computerisation? Technological Forecasting
        and Social Change 114: 254-280.
    Autor, D., Levy, F. & Murnane, R. (2003). The Skill Content of Recent
        Technological Change: An Empirical Exploration. QJE 118(4).
    Webb, M. (2020). The Impact of Artificial Intelligence on the Labor
        Market. Stanford working paper.

Score: high employment-weighted automation exposure -> STRESS/CRISIS.
Low exposure or rapid retraining capacity -> STABLE.
"""

import numpy as np

from app.layers.base import LayerBase


class AutomationExposure(LayerBase):
    layer_id = "l3"
    name = "Automation/AI Exposure"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params = [country, "automation_exposure"]
        if year:
            params.append(str(year))

        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = ?
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient automation data"}

        import json

        occupations = []

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            occ_name = meta.get("occupation", row.get("description", "unknown"))
            emp_share = meta.get("employment_share")
            fo_prob = row["value"]  # Frey-Osborne probability
            rti = meta.get("rti_index")
            ai_exposure = meta.get("ai_exposure")

            if fo_prob is None or emp_share is None:
                continue

            occupations.append({
                "name": occ_name,
                "emp_share": float(emp_share),
                "fo_probability": float(fo_prob),
                "rti_index": float(rti) if rti is not None else None,
                "ai_exposure": float(ai_exposure) if ai_exposure is not None else None,
            })

        if len(occupations) < 5:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient valid obs"}

        # Employment-weighted automation exposure
        shares = np.array([o["emp_share"] for o in occupations])
        fo_probs = np.array([o["fo_probability"] for o in occupations])

        # Normalize shares to sum to 1
        if np.sum(shares) > 0:
            shares = shares / np.sum(shares)

        weighted_fo = float(np.sum(shares * fo_probs))

        # RTI analysis
        rti_values = [o["rti_index"] for o in occupations if o["rti_index"] is not None]
        rti_shares = [o["emp_share"] for o in occupations if o["rti_index"] is not None]
        weighted_rti = None
        if rti_values:
            rti_arr = np.array(rti_values)
            rti_s = np.array(rti_shares)
            if np.sum(rti_s) > 0:
                rti_s = rti_s / np.sum(rti_s)
            weighted_rti = float(np.sum(rti_s * rti_arr))

        # AI exposure analysis
        ai_values = [o["ai_exposure"] for o in occupations if o["ai_exposure"] is not None]
        ai_shares = [o["emp_share"] for o in occupations if o["ai_exposure"] is not None]
        weighted_ai = None
        if ai_values:
            ai_arr = np.array(ai_values)
            ai_s = np.array(ai_shares)
            if np.sum(ai_s) > 0:
                ai_s = ai_s / np.sum(ai_s)
            weighted_ai = float(np.sum(ai_s * ai_arr))

        # Risk categories (Frey-Osborne thresholds)
        high_risk = [o for o in occupations if o["fo_probability"] > 0.7]
        medium_risk = [o for o in occupations if 0.3 <= o["fo_probability"] <= 0.7]
        low_risk = [o for o in occupations if o["fo_probability"] < 0.3]

        high_risk_emp_share = sum(o["emp_share"] for o in high_risk) / max(sum(shares), 1e-10)
        medium_risk_emp_share = sum(o["emp_share"] for o in medium_risk) / max(sum(shares), 1e-10)
        low_risk_emp_share = sum(o["emp_share"] for o in low_risk) / max(sum(shares), 1e-10)

        # Top 5 most exposed occupations by employment-weighted risk
        sorted_occs = sorted(occupations, key=lambda o: o["fo_probability"] * o["emp_share"], reverse=True)
        top_exposed = [
            {"occupation": o["name"], "probability": round(o["fo_probability"], 3),
             "employment_share": round(o["emp_share"], 4)}
            for o in sorted_occs[:5]
        ]

        # Score: weighted automation probability maps directly
        score = weighted_fo * 100.0
        # Amplify if AI exposure is also high (compounding risk)
        if weighted_ai is not None and weighted_ai > 0.5:
            score = min(100.0, score * 1.15)
        score = max(0.0, min(100.0, score))

        result = {
            "score": round(score, 2),
            "country": country,
            "n_occupations": len(occupations),
            "frey_osborne": {
                "weighted_probability": round(weighted_fo, 4),
                "high_risk_share": round(high_risk_emp_share, 4),
                "medium_risk_share": round(medium_risk_emp_share, 4),
                "low_risk_share": round(low_risk_emp_share, 4),
            },
            "top_exposed_occupations": top_exposed,
        }

        if weighted_rti is not None:
            result["routine_task_intensity"] = {
                "weighted_rti": round(weighted_rti, 4),
                "interpretation": (
                    "high routine content (automation vulnerable)" if weighted_rti > 0.5
                    else "moderate routine content" if weighted_rti > 0
                    else "low routine content (abstract/manual dominant)"
                ),
            }

        if weighted_ai is not None:
            result["ai_exposure"] = {
                "weighted_index": round(weighted_ai, 4),
                "interpretation": (
                    "high AI exposure (cognitive tasks at risk)" if weighted_ai > 0.5
                    else "moderate AI exposure" if weighted_ai > 0.3
                    else "low AI exposure"
                ),
            }

        return result
