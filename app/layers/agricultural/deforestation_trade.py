"""Deforestation-trade nexus: embodied deforestation in agricultural trade.

Quantifies the deforestation embodied in international trade of
forest-risk commodities. Global supply chains for soy, palm oil, beef,
cocoa, coffee, and rubber are major drivers of tropical deforestation.
This module links trade flow data to deforestation estimates in producing
regions.

Methodology:
    1. Identify forest-risk commodity trade flows for the target country
       (imports and exports).
    2. Apply commodity-specific deforestation intensity factors
       (hectares of forest lost per tonne of commodity produced) from
       published lifecycle assessments.
    3. Compute embodied deforestation: trade volume * deforestation intensity.
    4. Estimate trade-forest loss elasticity: % change in deforestation
       associated with 1% change in commodity trade volume.

Key deforestation intensity factors (ha/tonne, approximate):
    Soy (Brazil): 0.015 | Palm oil (Indonesia): 0.021
    Beef (Brazil): 0.87 | Cocoa (W. Africa): 0.025
    Coffee: 0.012 | Rubber (SE Asia): 0.018
    Timber: 0.40

Score (0-100): Higher score indicates greater embodied deforestation in
the country's agricultural trade portfolio.

References:
    Pendrill, F. et al. (2019). "Agricultural and forestry trade drives
        large share of tropical deforestation emissions." Global
        Environmental Change, 56, 1-10.
    Henders, S. et al. (2015). "Trading forests: land-use change and
        carbon emissions embodied in production and exports of forest-risk
        commodities." Environmental Research Letters, 10(12).
    Meyfroidt, P. et al. (2010). "Globalization of land use: distant
        drivers of land change and geographic displacement of land use."
        Current Opinion in Environmental Sustainability.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DeforestationTradeNexus(LayerBase):
    layer_id = "l5"
    name = "Deforestation-Trade Nexus"

    # Deforestation intensity: hectares of forest lost per tonne produced
    # Sources: Pendrill et al. (2019), Henders et al. (2015)
    DEFORESTATION_INTENSITY = {
        "soy": 0.015,        # Brazil Cerrado average
        "palm_oil": 0.021,   # Indonesia/Malaysia average
        "beef": 0.87,        # Brazil Amazon average
        "cocoa": 0.025,      # West Africa average
        "coffee": 0.012,     # Global average
        "rubber": 0.018,     # SE Asia average
        "timber": 0.40,      # Tropical timber
        "sugar_cane": 0.005, # Brazil
        "maize": 0.003,      # Global average
        "rice": 0.002,       # SE Asia paddy expansion
    }

    # Approximate world prices for quantity estimation (USD/tonne)
    COMMODITY_PRICES = {
        "soy": 450, "palm_oil": 900, "beef": 5000, "cocoa": 3000,
        "coffee": 3500, "rubber": 1800, "timber": 300, "sugar_cane": 350,
        "maize": 250, "rice": 400,
    }

    async def compute(self, db, **kwargs) -> dict:
        """Compute embodied deforestation in agricultural trade.

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

        # Fetch trade flows for forest-risk commodities
        import_data = {}
        export_data = {}

        for commodity in self.DEFORESTATION_INTENSITY:
            # Imports
            params_i = [country, f"%{commodity}%", "%import%"]
            if year:
                params_i.append(str(year))
            imp_rows = await db.fetch_all(
                f"""
                SELECT dp.value, ds.unit, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE ?
                  AND ds.description LIKE ?
                  AND ds.source IN ('comtrade', 'baci', 'fao')
                  {year_clause}
                ORDER BY dp.date DESC
                LIMIT 5
                """,
                tuple(params_i),
            )

            # Exports
            params_e = [country, f"%{commodity}%", "%export%"]
            if year:
                params_e.append(str(year))
            exp_rows = await db.fetch_all(
                f"""
                SELECT dp.value, ds.unit, dp.date
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.name LIKE ?
                  AND ds.description LIKE ?
                  AND ds.source IN ('comtrade', 'baci', 'fao')
                  {year_clause}
                ORDER BY dp.date DESC
                LIMIT 5
                """,
                tuple(params_e),
            )

            if imp_rows:
                import_data[commodity] = self._estimate_quantity(imp_rows, commodity)
            if exp_rows:
                export_data[commodity] = self._estimate_quantity(exp_rows, commodity)

        if not import_data and not export_data:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no forest-risk commodity trade data found",
            }

        # Compute embodied deforestation for imports (deforestation abroad)
        import_deforestation = {}
        total_import_defor = 0.0
        for commodity, quantity_tonnes in import_data.items():
            intensity = self.DEFORESTATION_INTENSITY[commodity]
            defor_ha = quantity_tonnes * intensity
            import_deforestation[commodity] = {
                "quantity_tonnes": round(quantity_tonnes, 2),
                "intensity_ha_per_tonne": intensity,
                "embodied_deforestation_ha": round(defor_ha, 2),
            }
            total_import_defor += defor_ha

        # Compute embodied deforestation for exports (deforestation at home)
        export_deforestation = {}
        total_export_defor = 0.0
        for commodity, quantity_tonnes in export_data.items():
            intensity = self.DEFORESTATION_INTENSITY[commodity]
            defor_ha = quantity_tonnes * intensity
            export_deforestation[commodity] = {
                "quantity_tonnes": round(quantity_tonnes, 2),
                "intensity_ha_per_tonne": intensity,
                "embodied_deforestation_ha": round(defor_ha, 2),
            }
            total_export_defor += defor_ha

        net_deforestation = total_import_defor - total_export_defor

        # Trade-forest loss elasticity
        elasticity_result = await self._estimate_elasticity(db, country)

        # Fetch country forest area for context
        forest_row = await db.fetch_one(
            """
            SELECT dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.name LIKE '%forest%area%'
              AND ds.source IN ('fao', 'wb', 'gfc')
            ORDER BY dp.date DESC LIMIT 1
            """,
            (country,),
        )
        forest_area = forest_row["value"] if forest_row else None

        # Score: higher embodied deforestation = higher score
        # Normalize by a reference: 100,000 ha = score 100
        reference_defor = 100_000.0
        total_defor = total_import_defor + total_export_defor
        score = max(0.0, min(100.0, (total_defor / reference_defor) * 100.0))

        # Adjust for net position: net exporter of deforestation gets higher penalty
        if total_export_defor > total_import_defor and total_defor > 0:
            export_share = total_export_defor / total_defor
            score = min(100.0, score * (1.0 + export_share * 0.3))

        return {
            "score": round(score, 2),
            "country": country,
            "year": year,
            "total_embodied_deforestation_ha": round(total_defor, 2),
            "import_embodied_deforestation_ha": round(total_import_defor, 2),
            "export_embodied_deforestation_ha": round(total_export_defor, 2),
            "net_deforestation_position_ha": round(net_deforestation, 2),
            "net_position": "deforestation_importer" if net_deforestation > 0 else "deforestation_exporter",
            "import_detail": import_deforestation,
            "export_detail": export_deforestation,
            "country_forest_area_ha": round(forest_area, 2) if forest_area else None,
            "deforestation_as_pct_forest": (
                round(total_defor / forest_area * 100, 6)
                if forest_area and forest_area > 0 else None
            ),
            "trade_forest_loss_elasticity": elasticity_result,
        }

    def _estimate_quantity(self, rows: list[dict], commodity: str) -> float:
        """Estimate trade quantity in tonnes from value or weight data."""
        total_tonnes = 0.0
        for row in rows:
            value = row["value"]
            unit = (row.get("unit") or "").lower()

            if "tonne" in unit or "ton" in unit:
                total_tonnes += value
            elif "kg" in unit:
                total_tonnes += value / 1000.0
            elif "usd" in unit or "$" in unit or not unit:
                # Convert value to quantity using world price
                price = self.COMMODITY_PRICES.get(commodity, 500)
                total_tonnes += value / price
            else:
                # Try value-based conversion as fallback
                price = self.COMMODITY_PRICES.get(commodity, 500)
                total_tonnes += value / price

        return total_tonnes

    async def _estimate_elasticity(self, db, country: str) -> dict | None:
        """Estimate trade-forest loss elasticity.

        Regress log(forest_loss) on log(ag_trade_value) over time.
        Elasticity = coefficient on log(trade).
        """
        # Forest loss series
        forest_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.name LIKE '%forest%loss%'
              AND ds.source IN ('gfc', 'fao', 'wb')
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        # Agricultural trade volume
        trade_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.name LIKE '%agri%trade%'
              AND ds.source IN ('comtrade', 'fao', 'wb')
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if len(forest_rows) < 5 or len(trade_rows) < 5:
            return None

        forest_by_date = {r["date"]: r["value"] for r in forest_rows}
        trade_by_date = {r["date"]: r["value"] for r in trade_rows}
        common = sorted(set(forest_by_date) & set(trade_by_date))

        if len(common) < 5:
            return None

        forest_vals = np.array([forest_by_date[d] for d in common], dtype=float)
        trade_vals = np.array([trade_by_date[d] for d in common], dtype=float)

        # Filter positive values for log transform
        valid = (forest_vals > 0) & (trade_vals > 0)
        if valid.sum() < 5:
            return None

        ln_forest = np.log(forest_vals[valid])
        ln_trade = np.log(trade_vals[valid])
        n = len(ln_forest)

        # OLS: ln(forest_loss) = a + b*ln(trade) + e
        X = np.column_stack([np.ones(n), ln_trade])
        try:
            beta = np.linalg.lstsq(X, ln_forest, rcond=None)[0]
        except np.linalg.LinAlgError:
            return None

        elasticity = beta[1]
        resid = ln_forest - X @ beta
        ss_res = float(np.sum(resid ** 2))
        ss_tot = float(np.sum((ln_forest - np.mean(ln_forest)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Standard error of elasticity
        sigma2 = ss_res / max(n - 2, 1)
        try:
            se = float(np.sqrt(sigma2 * np.linalg.inv(X.T @ X)[1, 1]))
        except np.linalg.LinAlgError:
            se = None

        return {
            "elasticity": round(float(elasticity), 4),
            "std_error": round(se, 4) if se is not None else None,
            "r_squared": round(r2, 4),
            "n_obs": n,
            "interpretation": (
                f"1% increase in ag trade associated with {abs(elasticity):.2f}% "
                f"{'increase' if elasticity > 0 else 'decrease'} in forest loss"
            ),
        }
