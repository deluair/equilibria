"""Inflation Decomposition - Core/food/energy/shelter split, trimmed mean, sticky vs flexible."""

from __future__ import annotations

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class InflationDecomposition(LayerBase):
    layer_id = "l2"
    name = "Inflation Decomposition"
    weight = 0.05

    # CPI component series (FRED)
    CPI_COMPONENTS = {
        "headline": "CPIAUCSL",      # CPI All Items
        "core": "CPILFESL",          # CPI Less Food and Energy
        "food": "CPIUFDSL",          # CPI Food
        "energy": "CPIENGSL",        # CPI Energy
        "shelter": "CUSR0000SAH1",   # CPI Shelter
        "medical": "CPIMEDSL",       # CPI Medical Care
        "transportation": "CPITRNSL",  # CPI Transportation
    }

    # PCE deflator series
    PCE_COMPONENTS = {
        "headline": "PCEPI",       # PCE Price Index
        "core": "PCEPILFE",        # Core PCE
    }

    # Trimmed-mean and sticky/flexible (Dallas Fed, Atlanta Fed)
    SPECIAL_MEASURES = {
        "trimmed_mean_pce": "PCETRIM12M159SFRBDAL",  # Dallas Fed 12-month trimmed mean PCE
        "sticky_cpi": "STICKCPIM157SFRBATL",         # Atlanta Fed sticky CPI
        "flexible_cpi": "FLEXCPIM157SFRBATL",        # Atlanta Fed flexible CPI
        "median_cpi": "MEDCPIM158SFRBCLE",           # Cleveland Fed median CPI
    }

    # CPI component weights (approximate, updated periodically)
    COMPONENT_WEIGHTS = {
        "food": 0.137,
        "energy": 0.070,
        "shelter": 0.345,
        "medical": 0.069,
        "transportation": 0.057,
        "core_other": 0.322,  # residual
    }

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "USA")
        lookback = kwargs.get("lookback_years", 10)

        all_series = {**self.CPI_COMPONENTS, **self.PCE_COMPONENTS, **self.SPECIAL_MEASURES}
        series_ids = list(all_series.values())

        rows = await db.execute_fetchall(
            """
            SELECT series_id, date, value FROM data_points
            WHERE series_id IN ({})
              AND country_code = ?
              AND date >= date('now', ?)
            ORDER BY series_id, date
            """.format(",".join("?" for _ in series_ids)),
            (*series_ids, country, f"-{lookback} years"),
        )

        series_map: dict[str, list[tuple[str, float]]] = {}
        for r in rows:
            series_map.setdefault(r["series_id"], []).append(
                (r["date"], float(r["value"]))
            )

        results = {}

        # Compute YoY inflation rates for CPI components
        cpi_rates = {}
        for name, sid in self.CPI_COMPONENTS.items():
            if sid in series_map and len(series_map[sid]) >= 13:
                vals = [v for _, v in series_map[sid]]
                # 12-month (YoY) percentage change
                yoy = [(vals[i] / vals[i - 12] - 1) * 100 for i in range(12, len(vals))]
                dates = [d for d, _ in series_map[sid][12:]]
                cpi_rates[name] = {
                    "current": yoy[-1],
                    "mean": float(np.mean(yoy)),
                    "std": float(np.std(yoy, ddof=1)),
                    "min": float(np.min(yoy)),
                    "max": float(np.max(yoy)),
                    "series": [
                        {"date": d, "value": v}
                        for d, v in zip(dates[-60:], yoy[-60:])
                    ],
                }

        results["cpi_components"] = cpi_rates

        # Compute PCE rates
        pce_rates = {}
        for name, sid in self.PCE_COMPONENTS.items():
            if sid in series_map and len(series_map[sid]) >= 13:
                vals = [v for _, v in series_map[sid]]
                yoy = [(vals[i] / vals[i - 12] - 1) * 100 for i in range(12, len(vals))]
                pce_rates[name] = float(yoy[-1]) if yoy else None
        results["pce_rates"] = pce_rates

        # Special measures
        special = {}
        for name, sid in self.SPECIAL_MEASURES.items():
            if sid in series_map:
                vals = [v for _, v in series_map[sid]]
                special[name] = float(vals[-1]) if vals else None
        results["special_measures"] = special

        # Trimmed mean CPI (compute from components if not available from FRED)
        if "headline" in cpi_rates:
            headline_rate = cpi_rates["headline"]["current"]
            core_rate = cpi_rates.get("core", {}).get("current")

            # Contribution decomposition: weight * component rate
            contributions = {}
            for comp, weight in self.COMPONENT_WEIGHTS.items():
                if comp in cpi_rates:
                    contributions[comp] = {
                        "rate": cpi_rates[comp]["current"],
                        "weight": weight,
                        "contribution": cpi_rates[comp]["current"] * weight,
                    }

            results["contributions"] = contributions

            # Core-headline spread
            if core_rate is not None:
                results["core_headline_spread"] = headline_rate - core_rate

        # Sticky vs flexible analysis
        if special.get("sticky_cpi") is not None and special.get("flexible_cpi") is not None:
            results["sticky_flexible_spread"] = special["sticky_cpi"] - special["flexible_cpi"]
            # Sticky inflation is more persistent, signals future inflation trend
            results["inflation_persistence_signal"] = (
                "elevated" if special["sticky_cpi"] > 3.0 else
                "moderate" if special["sticky_cpi"] > 2.0 else
                "contained"
            )

        # Inflation momentum: 3-month annualized vs 12-month
        if "headline" in self.CPI_COMPONENTS and self.CPI_COMPONENTS["headline"] in series_map:
            vals = [v for _, v in series_map[self.CPI_COMPONENTS["headline"]]]
            if len(vals) >= 13:
                yoy_12m = (vals[-1] / vals[-13] - 1) * 100
                ann_3m = ((vals[-1] / vals[-4]) ** 4 - 1) * 100 if len(vals) >= 4 else yoy_12m
                results["momentum"] = {
                    "yoy_12m": yoy_12m,
                    "annualized_3m": ann_3m,
                    "accelerating": ann_3m > yoy_12m,
                }

        # Score: deviation from 2% target
        headline = cpi_rates.get("headline", {}).get("current", 2.0)
        core = cpi_rates.get("core", {}).get("current", headline)

        # Asymmetric: above target is more stressful than below
        deviation = abs(core - 2.0)
        above_target = core > 2.0
        if above_target:
            # 2% = 0 stress, 4% = 50, 6% = 75, 8%+ = 90+
            score = float(np.clip(deviation * 18.0, 0, 100))
        else:
            # Below target (deflation risk): 2% = 0, 0% = 40, -1% = 70
            score = float(np.clip(deviation * 20.0, 0, 100))

        return {
            "score": score,
            "results": results,
        }
