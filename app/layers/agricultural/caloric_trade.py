"""Caloric trade balance: converting trade values to caloric equivalents.

Maps agricultural trade flows to caloric content using FAO food composition
tables, enabling analysis of a country's net caloric position in global
food trade. This reveals food dependency that monetary trade values can
mask (e.g., a country may be a net food exporter by value but a net
caloric importer if it exports high-value/low-calorie goods and imports
staple calories).

Methodology:
    1. Map HS6 product codes to FAO commodity groups.
    2. Apply caloric conversion factors (kcal per kg) from FAO food
       composition tables.
    3. Convert trade values to estimated quantities using unit values.
    4. Compute caloric equivalents of exports and imports.
    5. Net caloric balance = caloric exports - caloric imports.
    6. Food dependency ratio = caloric imports / (production + imports).

Key conversion factors (kcal/kg, approximate):
    Wheat: 3,390 | Rice (milled): 3,600 | Maize: 3,560
    Soybeans: 4,460 | Palm oil: 8,840 | Sugar: 3,870
    Beef: 2,500 | Poultry: 2,390 | Milk: 610

Score (0-100): Higher score indicates greater caloric dependence on imports.
A net caloric importer with high dependency scores toward CRISIS.

References:
    FAO (2012). "Food Composition Tables." INFOODS.
    D'Odorico, P. et al. (2014). "Feeding humanity through global food
        trade." Earth's Future, 2(9), 458-469.
"""

from __future__ import annotations

import numpy as np
from app.layers.base import LayerBase


class CaloricTradeBalance(LayerBase):
    layer_id = "l5"
    name = "Caloric Trade Balance"

    # Caloric conversion factors: kcal per kg (FAO food composition tables)
    KCAL_PER_KG = {
        "wheat": 3390,
        "rice": 3600,
        "maize": 3560,
        "soybeans": 4460,
        "palm_oil": 8840,
        "sugar": 3870,
        "beef": 2500,
        "poultry": 2390,
        "pork": 2420,
        "fish": 1200,
        "milk": 610,
        "eggs": 1550,
        "potatoes": 770,
        "cassava": 1600,
        "bananas": 890,
        "beans": 3410,
        "lentils": 3520,
        "groundnuts": 5670,
        "sunflower_oil": 8840,
        "soybean_oil": 8840,
        "butter": 7170,
        "cheese": 3560,
    }

    async def compute(self, db, **kwargs) -> dict:
        """Compute caloric trade balance.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code
            year : int - reference year
        """
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""

        # Fetch agricultural export data by commodity
        export_params = [country, "export", "agri%"]
        if year:
            export_params.append(str(year))

        export_rows = await db.fetch_all(
            f"""
            SELECT ds.name, dp.value, ds.unit, ds.metadata, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.description LIKE '%' || ? || '%'
              AND ds.name LIKE ?
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(export_params),
        )

        import_params = [country, "import", "agri%"]
        if year:
            import_params.append(str(year))

        import_rows = await db.fetch_all(
            f"""
            SELECT ds.name, dp.value, ds.unit, ds.metadata, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.description LIKE '%' || ? || '%'
              AND ds.name LIKE ?
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(import_params),
        )

        # Also try structured trade data
        trade_rows = await db.fetch_all(
            """
            SELECT ds.name, dp.value, ds.unit, ds.metadata, ds.description
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('comtrade', 'fao', 'baci')
              AND ds.name LIKE '%agri%'
            ORDER BY dp.date DESC
            LIMIT 500
            """,
            (country,),
        )

        # Convert trade flows to caloric equivalents
        export_kcal = self._compute_caloric_content(export_rows)
        import_kcal = self._compute_caloric_content(import_rows)

        # Also process structured trade rows
        export_struct, import_struct = self._split_trade_rows(trade_rows)
        export_kcal_s = self._compute_caloric_content(export_struct)
        import_kcal_s = self._compute_caloric_content(import_struct)

        # Combine
        total_export_kcal = export_kcal["total_kcal"] + export_kcal_s["total_kcal"]
        total_import_kcal = import_kcal["total_kcal"] + import_kcal_s["total_kcal"]
        export_by_commodity = {**export_kcal["by_commodity"], **export_kcal_s["by_commodity"]}
        import_by_commodity = {**import_kcal["by_commodity"], **import_kcal_s["by_commodity"]}

        if total_export_kcal == 0 and total_import_kcal == 0:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no agricultural trade data with caloric mapping",
            }

        # Net caloric balance (positive = net exporter)
        net_balance = total_export_kcal - total_import_kcal

        # Fetch domestic production for dependency ratio
        production_rows = await db.fetch_all(
            """
            SELECT ds.name, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'fao'
              AND ds.name LIKE '%production%quantity%'
            ORDER BY dp.date DESC
            LIMIT 50
            """,
            (country,),
        )
        production_kcal = self._compute_production_kcal(production_rows)

        # Food dependency ratio
        total_available = production_kcal + total_import_kcal
        dependency_ratio = (
            total_import_kcal / total_available if total_available > 0 else 0.0
        )

        # Self-sufficiency ratio
        self_sufficiency = (
            production_kcal / (production_kcal + total_import_kcal - total_export_kcal)
            if (production_kcal + total_import_kcal - total_export_kcal) > 0
            else 0.0
        )

        # Classification
        if net_balance > 0:
            classification = "net_caloric_exporter"
        else:
            classification = "net_caloric_importer"

        # Score: higher dependency = higher score
        # dependency_ratio near 1 = fully dependent (score 100)
        # Also penalize for negative net balance
        import_intensity = total_import_kcal / max(total_export_kcal, 1)
        score = max(0.0, min(100.0, dependency_ratio * 70.0 + min(import_intensity, 1.0) * 30.0))

        # Convert to billions of kcal for readability
        to_billion = 1e-9

        return {
            "score": round(score, 2),
            "country": country,
            "year": year,
            "classification": classification,
            "net_caloric_balance_billion_kcal": round(net_balance * to_billion, 4),
            "export_kcal_billion": round(total_export_kcal * to_billion, 4),
            "import_kcal_billion": round(total_import_kcal * to_billion, 4),
            "production_kcal_billion": round(production_kcal * to_billion, 4),
            "dependency_ratio": round(dependency_ratio, 4),
            "self_sufficiency_ratio": round(self_sufficiency, 4),
            "exports_by_commodity": {
                k: round(v * to_billion, 4) for k, v in export_by_commodity.items() if v > 0
            },
            "imports_by_commodity": {
                k: round(v * to_billion, 4) for k, v in import_by_commodity.items() if v > 0
            },
        }

    def _compute_caloric_content(self, rows: list[dict]) -> dict:
        """Convert trade rows to caloric equivalents using commodity matching."""
        total_kcal = 0.0
        by_commodity = {}

        for row in rows:
            name = (row.get("name") or "").lower()
            value = row.get("value", 0)
            unit = (row.get("unit") or "").lower()

            # Match commodity name to caloric factor
            matched_commodity = None
            kcal_factor = 0
            for commodity, factor in self.KCAL_PER_KG.items():
                if commodity.replace("_", " ") in name or commodity.replace("_", "") in name:
                    matched_commodity = commodity
                    kcal_factor = factor
                    break

            if matched_commodity is None or kcal_factor == 0:
                continue

            # Determine quantity in kg
            if "kg" in unit or "tonne" in unit or "ton" in unit:
                # value is already in weight
                quantity_kg = value * (1000 if "tonne" in unit or "ton" in unit else 1)
            elif "usd" in unit or "$" in unit:
                # Estimate quantity from value using approximate world prices (USD/tonne)
                approx_prices = {
                    "wheat": 300, "rice": 400, "maize": 250, "soybeans": 450,
                    "palm_oil": 900, "sugar": 350, "beef": 5000, "poultry": 2500,
                    "pork": 2000, "fish": 3000, "milk": 400, "eggs": 2000,
                    "potatoes": 200, "cassava": 150, "bananas": 500,
                    "beans": 700, "lentils": 800, "groundnuts": 1200,
                    "sunflower_oil": 1100, "soybean_oil": 1000, "butter": 4500,
                    "cheese": 4000,
                }
                price_per_tonne = approx_prices.get(matched_commodity, 500)
                quantity_kg = (value / price_per_tonne) * 1000
            else:
                continue

            kcal = quantity_kg * kcal_factor
            total_kcal += kcal
            by_commodity[matched_commodity] = by_commodity.get(matched_commodity, 0) + kcal

        return {"total_kcal": total_kcal, "by_commodity": by_commodity}

    def _compute_production_kcal(self, rows: list[dict]) -> float:
        """Convert production quantity rows to total caloric content."""
        total = 0.0
        for row in rows:
            name = (row.get("name") or "").lower()
            value = row.get("value", 0)
            for commodity, factor in self.KCAL_PER_KG.items():
                if commodity.replace("_", " ") in name or commodity.replace("_", "") in name:
                    # Production values in FAO are typically in tonnes
                    total += value * 1000 * factor
                    break
        return total

    @staticmethod
    def _split_trade_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
        """Split generic trade rows into exports and imports by description."""
        exports = []
        imports = []
        for row in rows:
            desc = (row.get("description") or "").lower()
            if "export" in desc:
                exports.append(row)
            elif "import" in desc:
                imports.append(row)
        return exports, imports
