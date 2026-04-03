"""Bioeconomy employment: life sciences, agriculture, and forestry employment share.

The bioeconomy's labor market spans primary biological sectors (agriculture, forestry,
fishing), bio-processing industries (food, beverages, bio-chemicals), and knowledge
sectors (life sciences, biotech R&D, pharmaceutical manufacturing). A high combined
employment share with rising productivity signals a vibrant, employment-generating
bioeconomy.

Score: high bioeconomy employment share with rising productivity -> STABLE (broad
economic engagement), high share with stagnant productivity -> WATCH (employment
trap), low share with declining agri-employment and no service offset -> STRESS.

Proxies: employment in agriculture (% total employment) as primary bioeconomy
employment, combined with services employment to estimate knowledge-bioeconomy share.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class BioeconomyEmployment(LayerBase):
    layer_id = "lBI"
    name = "Bioeconomy Employment"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        agri_emp_code = "SL.AGR.EMPL.ZS"
        service_emp_code = "SL.SRV.EMPL.ZS"
        agri_prod_code = "NV.AGR.TOTL.ZS"

        agri_emp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (agri_emp_code, "%Employment in agriculture%"),
        )
        service_emp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (service_emp_code, "%Employment in services%"),
        )
        agri_prod_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (agri_prod_code, "%Agriculture, forestry%"),
        )

        agri_emp_vals = [r["value"] for r in agri_emp_rows if r["value"] is not None]
        service_emp_vals = [r["value"] for r in service_emp_rows if r["value"] is not None]
        agri_prod_vals = [r["value"] for r in agri_prod_rows if r["value"] is not None]

        if not agri_emp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for agricultural employment SL.AGR.EMPL.ZS",
            }

        agri_emp = agri_emp_vals[0]
        service_emp = service_emp_vals[0] if service_emp_vals else None
        agri_prod = agri_prod_vals[0] if agri_prod_vals else None

        # Employment productivity gap: high agri employment with low agri GDP share
        # = low productivity, trapped labor (stress signal)
        if agri_prod is not None and agri_prod > 0:
            productivity_ratio = agri_emp / agri_prod  # employment/output ratio
        else:
            productivity_ratio = None

        # Base score from agri employment share
        # Very high (>50%) = subsistence-dominated, low productivity = higher stress
        if agri_emp >= 50:
            base = 60.0 + (agri_emp - 50) * 0.5
        elif agri_emp >= 25:
            base = 40.0 + (agri_emp - 25) * 0.8
        elif agri_emp >= 10:
            base = 25.0 + (agri_emp - 10) * 1.0
        elif agri_emp >= 3:
            base = 12.0 + (agri_emp - 3) * 1.86
        else:
            base = 10.0

        # Productivity ratio: high employment relative to output = inefficiency
        if productivity_ratio is not None:
            if productivity_ratio > 5:
                base = min(100.0, base + 15.0)
            elif productivity_ratio < 1:
                base = max(5.0, base - 10.0)

        # Service sector proxy for knowledge-bioeconomy (life sciences)
        # High services with moderate agri = likely life-sciences employment
        if service_emp is not None and service_emp >= 60 and agri_emp < 20:
            base = max(5.0, base - 8.0)

        score = round(min(100.0, base), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "agri_employment_pct": round(agri_emp, 2),
                "service_employment_pct": round(service_emp, 2) if service_emp is not None else None,
                "agri_value_added_gdp_pct": round(agri_prod, 2) if agri_prod is not None else None,
                "employment_productivity_ratio": round(productivity_ratio, 3) if productivity_ratio is not None else None,
                "n_obs_agri_emp": len(agri_emp_vals),
                "n_obs_service_emp": len(service_emp_vals),
            },
        }
