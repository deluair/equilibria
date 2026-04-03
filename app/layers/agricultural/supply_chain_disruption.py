"""Agricultural supply chain vulnerability and disruption index.

Assesses vulnerability of a country's agricultural supply chain through
concentration risk in import sources, stockpile adequacy, import dependency,
and logistical fragility.

Methodology:
    1. Import concentration (Herfindahl-Hirschman Index):
       HHI = sum(s_i^2) for each source country share s_i
       HHI > 2500 = highly concentrated (vulnerable)

    2. Stockpile adequacy:
       months_of_coverage = stocks / monthly_consumption
       Below 2 months = critical, below 4 = warning

    3. Import dependency ratio:
       IDR = imports / (production + imports - exports)
       High IDR = vulnerable to supply disruptions

    4. Source diversification entropy:
       H = -sum(s_i * ln(s_i))
       Low entropy = concentrated sourcing

    5. Supply chain resilience composite:
       Weighted combination of HHI, stockpile adequacy, IDR, and
       infrastructure quality index.

    Score: high concentration + low stocks + high import dependency
    = food security stress.

References:
    Headey, D. (2011). "Rethinking the global food crisis: The role of
        trade shocks." Food Policy, 36(2), 136-146.
    Puma, M.J. et al. (2015). "Assessing the evolving fragility of the
        global food system." Environmental Research Letters, 10(2).
    Kummu, M. et al. (2020). "Interplay of trade and food system
        resilience." Global Food Security, 24, 100347.
    FAO (2023). "The State of Food Security and Nutrition in the World."
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class SupplyChainDisruption(LayerBase):
    layer_id = "l5"
    name = "Supply Chain Disruption"

    # Critical food commodities for supply chain assessment
    CRITICAL_COMMODITIES = [
        "rice", "wheat", "maize", "edible_oil", "sugar",
        "pulses", "dairy", "fertilizer",
    ]

    async def compute(self, db, **kwargs) -> dict:
        """Compute agricultural supply chain vulnerability index.

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
            SELECT dp.value, ds.description, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'food_supply_chain'
              AND ds.country_iso3 = ?
              {year_clause}
            ORDER BY ds.description, dp.date DESC
            """,
            tuple(params),
        )

        if not rows or len(rows) < 5:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient supply chain data"}

        import json

        # Parse into commodity-level supply chain data
        commodity_data: dict[str, dict] = {}
        import_sources: dict[str, dict[str, float]] = {}  # commodity -> {source: value}

        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            commodity = row["description"] or meta.get("commodity", "unknown")

            if commodity not in commodity_data:
                commodity_data[commodity] = {
                    "production": 0.0,
                    "imports": 0.0,
                    "exports": 0.0,
                    "stocks": 0.0,
                    "consumption": 0.0,
                }

            data_type = meta.get("type")
            value = float(row["value"]) if row["value"] else 0.0

            if data_type == "production":
                commodity_data[commodity]["production"] = value
            elif data_type == "imports":
                commodity_data[commodity]["imports"] = value
                source = meta.get("source_country")
                if source:
                    import_sources.setdefault(commodity, {})[source] = (
                        import_sources.get(commodity, {}).get(source, 0) + value
                    )
            elif data_type == "exports":
                commodity_data[commodity]["exports"] = value
            elif data_type == "stocks":
                commodity_data[commodity]["stocks"] = value
            elif data_type == "consumption":
                commodity_data[commodity]["consumption"] = value

        if not commodity_data:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "no commodity supply data parsed"}

        results_by_commodity = {}
        all_hhi = []
        all_idr = []
        all_stock_months = []

        for comm, data in commodity_data.items():
            prod = data["production"]
            imp = data["imports"]
            exp = data["exports"]
            stocks = data["stocks"]
            consumption = data["consumption"]

            # Import dependency ratio
            total_supply = prod + imp - exp
            idr = imp / total_supply if total_supply > 0 else 0.0

            # Self-sufficiency ratio
            ssr = prod / consumption if consumption > 0 else 1.0

            # Stockpile adequacy (months of consumption)
            monthly_consumption = consumption / 12 if consumption > 0 else 1.0
            stock_months = stocks / monthly_consumption if monthly_consumption > 0 else 0.0

            # Import source HHI
            sources = import_sources.get(comm, {})
            hhi = 0.0
            entropy = 0.0
            n_sources = len(sources)
            top_source = None
            top_share = 0.0

            if sources:
                total_imports = sum(sources.values())
                if total_imports > 0:
                    shares = {k: v / total_imports for k, v in sources.items()}
                    hhi = sum(s ** 2 for s in shares.values()) * 10000  # scale to 0-10000
                    entropy = -sum(s * np.log(s) for s in shares.values() if s > 0)
                    top_source = max(shares, key=shares.get)
                    top_share = shares[top_source]

            all_hhi.append(hhi)
            all_idr.append(idr)
            all_stock_months.append(stock_months)

            # Commodity-level vulnerability
            hhi_risk = "high" if hhi > 2500 else "moderate" if hhi > 1500 else "low"
            stock_risk = "critical" if stock_months < 2 else "warning" if stock_months < 4 else "adequate"
            idr_risk = "high" if idr > 0.5 else "moderate" if idr > 0.2 else "low"

            results_by_commodity[comm] = {
                "production": round(prod, 1),
                "imports": round(imp, 1),
                "exports": round(exp, 1),
                "stocks": round(stocks, 1),
                "consumption": round(consumption, 1),
                "import_dependency_ratio": round(float(idr), 3),
                "self_sufficiency_ratio": round(float(ssr), 3),
                "stock_months_coverage": round(float(stock_months), 1),
                "import_concentration": {
                    "hhi": round(float(hhi), 0),
                    "entropy": round(float(entropy), 3),
                    "n_sources": n_sources,
                    "top_source": top_source,
                    "top_source_share": round(float(top_share), 3) if top_source else None,
                },
                "risk_levels": {
                    "concentration": hhi_risk,
                    "stockpile": stock_risk,
                    "import_dependency": idr_risk,
                },
            }

        # Aggregate vulnerability index
        avg_hhi = float(np.mean(all_hhi)) if all_hhi else 0.0
        avg_idr = float(np.mean(all_idr)) if all_idr else 0.0
        avg_stock_months = float(np.mean(all_stock_months)) if all_stock_months else 12.0

        # Component scores (each 0-33.3)
        hhi_score = float(np.clip(avg_hhi / 10000 * 33.3, 0, 33.3))
        idr_score = float(np.clip(avg_idr * 33.3, 0, 33.3))
        stock_score = float(np.clip((6 - avg_stock_months) / 6 * 33.3, 0, 33.3))
        score = float(np.clip(hhi_score + idr_score + stock_score, 0, 100))

        # Critical commodities at risk
        critical_risks = []
        for comm in self.CRITICAL_COMMODITIES:
            if comm in results_by_commodity:
                r = results_by_commodity[comm]
                if (r["risk_levels"]["concentration"] == "high"
                        or r["risk_levels"]["stockpile"] == "critical"):
                    critical_risks.append(comm)

        return {
            "score": round(score, 2),
            "country": country,
            "n_commodities": len(commodity_data),
            "aggregate": {
                "mean_hhi": round(avg_hhi, 0),
                "mean_import_dependency": round(avg_idr, 3),
                "mean_stock_months": round(avg_stock_months, 1),
                "critical_commodities_at_risk": critical_risks,
            },
            "component_scores": {
                "concentration": round(hhi_score, 2),
                "import_dependency": round(idr_score, 2),
                "stockpile_adequacy": round(stock_score, 2),
            },
            "commodities": results_by_commodity,
        }
