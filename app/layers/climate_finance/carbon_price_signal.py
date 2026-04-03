"""Carbon price signal: effective carbon price vs social cost of carbon.

Methodology
-----------
**Effective carbon price** (OECD, 2021):
    Combines explicit carbon taxes, ETS prices, and fuel excise taxes net of
    fossil fuel subsidies, converted to a per-tonne CO2e basis:

        ECP = carbon_tax_rate + (ets_price * ets_coverage)
              + fuel_excise_co2_equivalent - subsidy_co2_equivalent

    Coverage rate = share of emissions covered by any carbon pricing instrument.
    Effective rate = weighted average price across covered emissions.

**Social cost of carbon** (US EPA, 2023 revision):
    SCC = $190/tCO2 at 2% discount rate (central), range $44-$413.
    Ramsey discounting with eta=1.5, rho=0.001 gives higher values.
    EU uses ~€50-130/tCO2 in policy appraisal.

**Price adequacy ratio**:
    ratio = ECP / SCC
    ratio >= 1.0: carbon price internalizes full social cost
    ratio < 0.5: severe underpricing, market failure

**Carbon leakage risk**:
    Differential pricing across jurisdictions induces production relocation.
    Leakage rate proxied by trade exposure of covered sectors.

Score: underpriced carbon raises the score (worse = more underpricing).
ECP/SCC = 1.0 -> score 0; no carbon price -> score 90+.

Sources: OECD Effective Carbon Rates, World Bank Carbon Pricing Dashboard,
IMF, US EPA Social Cost of Carbon Technical Support Document 2023
"""

from app.layers.base import LayerBase

_SQL = """
    SELECT dp.date, dp.value
    FROM data_points dp
    JOIN data_series ds ON dp.series_id = ds.id
    WHERE ds.code = ?
    ORDER BY dp.date
"""


class CarbonPriceSignal(LayerBase):
    layer_id = "lGF"
    name = "Carbon Price Signal"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country", "WLD")
        scc = kwargs.get("social_cost_carbon", 190.0)  # $/tCO2, EPA 2023 central

        codes = {
            "carbon_tax": f"CARBON_TAX_RATE_{country}",
            "ets_price": f"ETS_PRICE_{country}",
            "ets_coverage": f"ETS_COVERAGE_{country}",
            "fuel_excise_co2eq": f"FUEL_EXCISE_CO2EQ_{country}",
            "fossil_subsidy_co2eq": f"FOSSIL_SUBSIDY_CO2EQ_{country}",
            "emissions_covered_pct": f"EMISSIONS_CARBON_PRICED_PCT_{country}",
        }

        data: dict[str, dict] = {}
        for key, code in codes.items():
            rows = await db.fetch_all(_SQL, (code,))
            if rows:
                data[key] = {r["date"]: float(r["value"]) for r in rows}

        if not any(k in data for k in ("carbon_tax", "ets_price")):
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No carbon price data (carbon_tax or ets_price)",
            }

        # Collect latest available values
        def latest_val(key: str) -> float | None:
            if key not in data:
                return None
            vals = list(data[key].values())
            return float(vals[-1]) if vals else None

        carbon_tax = latest_val("carbon_tax") or 0.0
        ets_price = latest_val("ets_price") or 0.0
        ets_coverage = (latest_val("ets_coverage") or 0.0) / 100
        fuel_excise = latest_val("fuel_excise_co2eq") or 0.0
        subsidy_co2eq = latest_val("fossil_subsidy_co2eq") or 0.0
        coverage_pct = latest_val("emissions_covered_pct") or 0.0

        ecp = carbon_tax + (ets_price * ets_coverage) + fuel_excise - subsidy_co2eq
        ecp = max(ecp, 0.0)

        adequacy_ratio = ecp / scc if scc > 0 else 0.0

        # Score: underprice -> high score
        # ratio >= 1.0 -> 0 pts from gap; ratio 0 -> 90 pts
        price_gap_score = max(1 - adequacy_ratio, 0) * 70

        # Coverage penalty: low coverage of total emissions
        coverage_penalty = max(100 - coverage_pct, 0) * 0.3  # up to 30 pts

        score = min(price_gap_score + coverage_penalty, 100)

        return {
            "score": round(score, 1),
            "metrics": {
                "country": country,
                "effective_carbon_price_usd_tco2": round(ecp, 2),
                "social_cost_carbon_usd_tco2": round(scc, 2),
                "adequacy_ratio": round(adequacy_ratio, 3),
                "carbon_tax_usd_tco2": round(carbon_tax, 2),
                "ets_price_usd_tco2": round(ets_price, 2),
                "ets_coverage_pct": round(ets_coverage * 100, 1),
                "fuel_excise_co2eq_usd_tco2": round(fuel_excise, 2),
                "fossil_subsidy_co2eq_usd_tco2": round(subsidy_co2eq, 2),
                "emissions_covered_pct": round(coverage_pct, 1),
            },
        }
