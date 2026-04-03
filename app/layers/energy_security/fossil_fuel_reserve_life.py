"""Fossil fuel reserve life: proven reserves-to-production ratio for oil and gas.

The reserves-to-production (R/P) ratio measures how many years proven reserves
would last at current extraction rates. A declining R/P signals approaching
resource depletion and growing future supply insecurity. Proxied via WDI
NY.GDP.PETR.RT.ZS (oil rents % of GDP) and EG.ELC.FOSL.ZS (fossil fuel
electricity share) as reserve-life correlates when direct R/P is unavailable.

Score: high R/P (>50 yrs) -> STABLE, moderate (20-50) -> WATCH,
low (10-20) -> STRESS, critical (<10) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class FossilFuelReserveLife(LayerBase):
    layer_id = "lES"
    name = "Fossil Fuel Reserve Life"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        oil_code = "NY.GDP.PETR.RT.ZS"
        gas_code = "NY.GDP.NGAS.RT.ZS"

        oil_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (oil_code, "%oil rents%"),
        )
        gas_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gas_code, "%natural gas rents%"),
        )

        oil_vals = [r["value"] for r in oil_rows if r["value"] is not None]
        gas_vals = [r["value"] for r in gas_rows if r["value"] is not None]

        if not oil_vals and not gas_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for oil or gas rents (NY.GDP.PETR.RT.ZS, NY.GDP.NGAS.RT.ZS)",
            }

        oil_rent = oil_vals[0] if oil_vals else 0.0
        gas_rent = gas_vals[0] if gas_vals else 0.0

        # Combined fossil rents as a proxy for reserve richness relative to production.
        # High rents (>5% GDP) suggest abundant reserves relative to current output -> STABLE.
        # Low rents (near zero) suggest near-depletion or non-producer -> context-dependent.
        # Non-producers (both near 0) are scored separately as import-dependent.
        combined_rent = oil_rent + gas_rent

        # For producers: higher rents proxy for longer reserve life (lower stress).
        # For non-producers (combined < 0.1): score reflects supply insecurity, not depletion per se.
        if combined_rent >= 10.0:
            score = 5.0 + (20.0 - min(combined_rent, 20.0)) * 1.0
        elif combined_rent >= 5.0:
            score = 15.0 + (10.0 - combined_rent) * 2.0
        elif combined_rent >= 1.0:
            score = 35.0 + (5.0 - combined_rent) * 5.0
        elif combined_rent >= 0.1:
            score = 55.0 + (1.0 - combined_rent) * 11.1
        else:
            # Non-producer: high import insecurity on this dimension
            score = 70.0

        score = round(max(0.0, min(100.0, score)), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "oil_rents_gdp_pct": round(oil_rent, 3),
                "gas_rents_gdp_pct": round(gas_rent, 3),
                "combined_fossil_rents_gdp_pct": round(combined_rent, 3),
                "n_obs_oil": len(oil_vals),
                "n_obs_gas": len(gas_vals),
                "resource_status": (
                    "resource_rich" if combined_rent >= 10.0
                    else "moderate_producer" if combined_rent >= 1.0
                    else "marginal_producer" if combined_rent >= 0.1
                    else "non_producer"
                ),
            },
        }
