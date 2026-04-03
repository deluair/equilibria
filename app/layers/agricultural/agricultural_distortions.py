"""Agricultural trade distortion measurement.

Computes the Nominal Rate of Assistance (NRA), Consumer Tax Equivalent (CTE),
and Trade Bias Index following the Anderson-Valenzuela methodology developed
for the World Bank's agricultural distortions database.

Methodology:
    Nominal Rate of Assistance (NRA):
        NRA = (domestic_price - reference_price) / reference_price
        where reference_price is the undistorted border price adjusted for
        transport, handling, and quality differences.

        NRA > 0: protection (domestic price above world level)
        NRA < 0: taxation (domestic price below world level)

    Consumer Tax Equivalent (CTE):
        CTE = (consumer_price - reference_price) / reference_price
        Measures implicit tax (+) or subsidy (-) on consumers.

    Trade Bias Index (TBI):
        TBI = (1 + NRA_x) / (1 + NRA_m) - 1
        where NRA_x = NRA for exportables, NRA_m = NRA for import-competing.
        TBI > 0: anti-trade bias (exportables taxed relative to importables)
        TBI < 0: pro-trade bias

    Relative Rate of Assistance (RRA):
        RRA = (1 + NRA_ag) / (1 + NRA_nonag) - 1
        Measures agricultural policy bias relative to non-agriculture.

    Score: large absolute distortions indicate policy stress.

References:
    Anderson, K. & Valenzuela, E. (2008). "Estimates of Global Distortions
        to Agricultural Incentives, 1955 to 2007." World Bank.
    Anderson, K. (2009). "Distortions to Agricultural Incentives: A Global
        Perspective, 1955-2007." Palgrave Macmillan & World Bank.
    Krueger, A.O., Schiff, M. & Valdes, A. (1988). "Agricultural Incentives
        in Developing Countries: Measuring the Effect of Sectoral and
        Economywide Policies." World Bank Economic Review, 2(3), 255-271.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class AgriculturalDistortions(LayerBase):
    layer_id = "l5"
    name = "Agricultural Distortions"

    async def compute(self, db, **kwargs) -> dict:
        """Compute NRA, CTE, TBI for a country's agricultural sector.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            year : int - reference year
        """
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params = [country]
        if year:
            params.append(str(year))

        rows = await db.fetch_all(
            f"""
            SELECT dp.date AS year, dp.value, ds.description, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'agricultural_distortions'
              AND ds.country_iso3 = ?
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient agricultural distortion data"}

        import json

        # Parse commodity-level price data
        commodities: dict[str, dict] = {}
        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            commodity = row["description"] or meta.get("commodity", "unknown")
            domestic_price = meta.get("domestic_price")
            reference_price = meta.get("reference_price")
            consumer_price = meta.get("consumer_price")
            production_value = meta.get("production_value", 1.0)
            trade_status = meta.get("trade_status", "importable")  # exportable/importable/nontradable
            is_agricultural = meta.get("is_agricultural", True)

            if domestic_price is None or reference_price is None or reference_price <= 0:
                continue

            commodities[commodity] = {
                "domestic_price": float(domestic_price),
                "reference_price": float(reference_price),
                "consumer_price": float(consumer_price) if consumer_price else float(domestic_price),
                "production_value": float(production_value),
                "trade_status": trade_status,
                "is_agricultural": is_agricultural,
            }

        if len(commodities) < 2:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient commodity price pairs"}

        # Compute NRA and CTE for each commodity
        nra_by_commodity = {}
        cte_by_commodity = {}
        prod_weights = {}

        for name, data in commodities.items():
            nra = (data["domestic_price"] - data["reference_price"]) / data["reference_price"]
            cte = (data["consumer_price"] - data["reference_price"]) / data["reference_price"]
            nra_by_commodity[name] = nra
            cte_by_commodity[name] = cte
            prod_weights[name] = data["production_value"]

        # Production-weighted aggregate NRA
        total_prod = sum(prod_weights.values())
        if total_prod > 0:
            weights = {k: v / total_prod for k, v in prod_weights.items()}
        else:
            weights = {k: 1 / len(prod_weights) for k in prod_weights}

        nra_aggregate = sum(nra_by_commodity[k] * weights[k] for k in nra_by_commodity)
        cte_aggregate = sum(cte_by_commodity[k] * weights[k] for k in cte_by_commodity)

        # Trade Bias Index
        exportables = {k: v for k, v in commodities.items() if v["trade_status"] == "exportable"}
        importables = {k: v for k, v in commodities.items() if v["trade_status"] == "importable"}

        nra_x = self._weighted_nra(exportables, nra_by_commodity)
        nra_m = self._weighted_nra(importables, nra_by_commodity)

        tbi = None
        if nra_m is not None and (1 + nra_m) != 0:
            if nra_x is not None:
                tbi = (1 + nra_x) / (1 + nra_m) - 1

        # Relative Rate of Assistance (ag vs non-ag)
        ag_commodities = {k: v for k, v in commodities.items() if v["is_agricultural"]}
        nonag_commodities = {k: v for k, v in commodities.items() if not v["is_agricultural"]}
        nra_ag = self._weighted_nra(ag_commodities, nra_by_commodity)
        nra_nonag = self._weighted_nra(nonag_commodities, nra_by_commodity)

        rra = None
        if nra_ag is not None and nra_nonag is not None and (1 + nra_nonag) != 0:
            rra = (1 + nra_ag) / (1 + nra_nonag) - 1

        # Dispersion of NRA across commodities (policy inconsistency)
        nra_values = np.array(list(nra_by_commodity.values()))
        nra_dispersion = float(nra_values.std()) if len(nra_values) > 1 else 0.0

        # Anti-agricultural bias indicator
        anti_ag_bias = rra < -0.1 if rra is not None else None

        # Score: large distortions (absolute NRA), high dispersion, anti-ag bias = stress
        abs_nra = abs(float(nra_aggregate))
        distortion_score = float(np.clip(abs_nra * 100, 0, 50))
        dispersion_score = float(np.clip(nra_dispersion * 100, 0, 25))
        bias_score = 25.0 if (rra is not None and rra < -0.2) else 0.0
        score = float(np.clip(distortion_score + dispersion_score + bias_score, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_commodities": len(commodities),
            "nra": {
                "aggregate": round(float(nra_aggregate), 4),
                "by_commodity": {k: round(v, 4) for k, v in nra_by_commodity.items()},
                "exportables": round(float(nra_x), 4) if nra_x is not None else None,
                "importables": round(float(nra_m), 4) if nra_m is not None else None,
                "dispersion_std": round(nra_dispersion, 4),
            },
            "cte": {
                "aggregate": round(float(cte_aggregate), 4),
                "by_commodity": {k: round(v, 4) for k, v in cte_by_commodity.items()},
            },
            "trade_bias_index": round(float(tbi), 4) if tbi is not None else None,
            "relative_rate_of_assistance": round(float(rra), 4) if rra is not None else None,
            "anti_agricultural_bias": anti_ag_bias,
            "interpretation": {
                "nra_sign": "protection" if nra_aggregate > 0 else "taxation",
                "tbi_sign": (
                    "anti-trade bias" if tbi and tbi > 0
                    else "pro-trade bias" if tbi and tbi < 0
                    else "neutral"
                ),
            },
        }

    @staticmethod
    def _weighted_nra(
        commodity_subset: dict[str, dict],
        nra_map: dict[str, float],
    ) -> float | None:
        """Production-weighted NRA for a subset of commodities."""
        if not commodity_subset:
            return None
        total = sum(c["production_value"] for c in commodity_subset.values())
        if total <= 0:
            return float(np.mean([nra_map[k] for k in commodity_subset]))
        return sum(
            nra_map[k] * c["production_value"] / total
            for k, c in commodity_subset.items()
        )
