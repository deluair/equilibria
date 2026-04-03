"""Social protection targeting and adequacy analysis.

Implements core social protection analytics:

1. Targeting efficiency: measures how well transfer programs reach the
   intended beneficiaries.
   - Inclusion error (leakage): fraction of non-poor receiving transfers
   - Exclusion error (under-coverage): fraction of poor NOT receiving transfers
   - Targeting differential: E = coverage_poor - coverage_nonpoor

2. Transfer adequacy: ratio of average transfer to poverty gap.
       adequacy = avg_transfer / poverty_line
   Also measures the poverty gap reduction achieved.

3. Fiscal cost of Universal Basic Income (UBI):
       UBI_cost = population * ubi_amount * 12
       fiscal_cost_pct_gdp = UBI_cost / GDP * 100
   Net cost accounting for replaced programs and clawback via taxation.

4. Poverty trap interactions: effective marginal tax rate (EMTR) from
   benefit withdrawal as income rises.
       EMTR = (benefit_reduction + tax_increase) / income_increase
   EMTR > 60-80% creates poverty traps (high implicit marginal tax).

References:
    Coady, D., Grosh, M. & Hoddinott, J. (2004). Targeting of Transfers
        in Developing Countries. World Bank.
    Banerjee, A. et al. (2019). Universal Basic Income in the Developing
        World. Annual Review of Economics, 11, 959-983.
    Moffitt, R. (2002). Welfare Programs and Labor Supply. Handbook of
        Public Economics, 4, 2393-2430.
    Hanna, R. & Olken, B. (2018). Universal Basic Incomes versus Targeted
        Transfers. Journal of Economic Perspectives, 32(4), 201-226.

Sources: WDI (poverty, social protection), ILO (social spending)
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SocialProtection(LayerBase):
    layer_id = "l10"
    name = "Social Protection"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        results = {"country": country}

        # --- Targeting efficiency ---
        targeting_rows = await db.fetch_all(
            """
            SELECT dp.value, ds.metadata, ds.series_id
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id LIKE '%SOCIAL_PROTECTION%'
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        # Also fetch poverty headcount and population
        poverty_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SI.POV.DDAY', 'SI.POV.NAHC', 'POVERTY_HEADCOUNT')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        pop_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SP.POP.TOTL', 'POPULATION')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        gdp_rows = await db.fetch_all(
            """
            SELECT dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('NY.GDP.MKTP.CD', 'GDP_CURRENT')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        poverty_rate = float(poverty_rows[0]["value"]) / 100.0 if poverty_rows else None
        population = float(pop_rows[0]["value"]) if pop_rows else None
        gdp = float(gdp_rows[0]["value"]) if gdp_rows else None

        # Parse targeting data if available
        import json

        coverage_poor = kwargs.get("coverage_poor")
        coverage_nonpoor = kwargs.get("coverage_nonpoor")
        avg_transfer = kwargs.get("avg_transfer")

        for r in targeting_rows:
            meta = json.loads(r["metadata"]) if r.get("metadata") else {}
            sid = r["series_id"]
            if "COVERAGE_POOR" in sid and coverage_poor is None:
                coverage_poor = float(r["value"]) / 100.0
            elif "COVERAGE_NONPOOR" in sid and coverage_nonpoor is None:
                coverage_nonpoor = float(r["value"]) / 100.0
            elif "AVG_TRANSFER" in sid and avg_transfer is None:
                avg_transfer = float(r["value"])
            else:
                cp = meta.get("coverage_poor")
                cn = meta.get("coverage_nonpoor")
                at = meta.get("avg_transfer")
                if cp is not None and coverage_poor is None:
                    coverage_poor = float(cp)
                if cn is not None and coverage_nonpoor is None:
                    coverage_nonpoor = float(cn)
                if at is not None and avg_transfer is None:
                    avg_transfer = float(at)

        if coverage_poor is not None and coverage_nonpoor is not None:
            exclusion_error = 1.0 - coverage_poor  # fraction of poor not covered
            inclusion_error = coverage_nonpoor  # fraction of non-poor covered

            targeting_diff = coverage_poor - coverage_nonpoor

            results["targeting"] = {
                "coverage_poor": round(coverage_poor, 4),
                "coverage_nonpoor": round(coverage_nonpoor, 4),
                "exclusion_error": round(exclusion_error, 4),
                "inclusion_error": round(inclusion_error, 4),
                "targeting_differential": round(targeting_diff, 4),
                "quality": (
                    "well-targeted" if targeting_diff > 0.3 else "moderately-targeted" if targeting_diff > 0.1 else "poorly-targeted"
                ),
            }
        else:
            results["targeting"] = {"error": "no targeting data available"}

        # --- Transfer adequacy ---
        poverty_line = kwargs.get("poverty_line", 2.15)  # $2.15/day PPP (2017)

        if avg_transfer is not None:
            daily_transfer = avg_transfer / 30.0  # monthly to daily
            adequacy_ratio = daily_transfer / poverty_line if poverty_line > 0 else 0

            results["adequacy"] = {
                "avg_monthly_transfer": round(avg_transfer, 2),
                "daily_equivalent": round(daily_transfer, 2),
                "poverty_line_daily": poverty_line,
                "adequacy_ratio": round(adequacy_ratio, 4),
                "assessment": ("adequate" if adequacy_ratio >= 0.75 else "moderate" if adequacy_ratio >= 0.4 else "inadequate"),
            }
        else:
            results["adequacy"] = {"error": "no transfer amount data"}

        # --- UBI fiscal cost ---
        ubi_amount = kwargs.get("ubi_monthly", None)
        if ubi_amount is None and poverty_line is not None:
            ubi_amount = poverty_line * 30.0  # set UBI at poverty line

        if population and gdp and ubi_amount:
            annual_ubi_cost = population * ubi_amount * 12.0
            pct_gdp = (annual_ubi_cost / gdp) * 100.0 if gdp > 0 else 0

            # Net cost: subtract existing social protection spending
            social_spending_rows = await db.fetch_all(
                """
                SELECT dp.value
                FROM data_series ds
                JOIN data_points dp ON dp.series_id = ds.id
                WHERE ds.country_iso3 = ?
                  AND ds.series_id IN ('GC.XPN.TRFT.ZS', 'SOCIAL_SPENDING_GDP')
                ORDER BY dp.date DESC
                LIMIT 1
                """,
                (country,),
            )
            existing_pct = float(social_spending_rows[0]["value"]) if social_spending_rows else 0

            # Tax clawback from higher-income recipients (rough estimate)
            avg_tax_rate = kwargs.get("avg_income_tax_rate", 0.15)
            clawback_pct = pct_gdp * avg_tax_rate * 0.5  # ~half of UBI taxed back from upper half

            net_pct_gdp = pct_gdp - existing_pct - clawback_pct

            results["ubi_cost"] = {
                "ubi_monthly_per_person": round(ubi_amount, 2),
                "gross_annual_cost": round(annual_ubi_cost, 0),
                "gross_pct_gdp": round(pct_gdp, 2),
                "existing_social_spending_pct": round(existing_pct, 2),
                "tax_clawback_pct": round(clawback_pct, 2),
                "net_additional_pct_gdp": round(net_pct_gdp, 2),
                "poverty_rate": round(poverty_rate, 4) if poverty_rate else None,
                "feasibility": ("feasible" if net_pct_gdp < 3 else "challenging" if net_pct_gdp < 8 else "very costly"),
            }
        else:
            results["ubi_cost"] = {"error": "missing population, GDP, or UBI amount"}

        # --- Poverty trap interactions (EMTR) ---
        withdrawal_rate = kwargs.get("benefit_withdrawal_rate", 0.5)
        marginal_tax_rate = kwargs.get("marginal_tax_rate", 0.15)

        emtr = withdrawal_rate + marginal_tax_rate
        participation_tax_rate = emtr  # simplified for in/out-of-work

        # Threshold where working is barely worthwhile
        # At EMTR > 0.6, poverty trap risk is significant
        trap_risk = "high" if emtr > 0.7 else "moderate" if emtr > 0.5 else "low"

        # Calculate breakeven hours at minimum wage
        min_wage = kwargs.get("min_hourly_wage")
        if min_wage and avg_transfer:
            net_gain_per_hour = min_wage * (1.0 - emtr)
            breakeven_hours = avg_transfer / net_gain_per_hour if net_gain_per_hour > 0 else float("inf")
        else:
            net_gain_per_hour = None
            breakeven_hours = None

        results["poverty_trap"] = {
            "benefit_withdrawal_rate": withdrawal_rate,
            "marginal_tax_rate": marginal_tax_rate,
            "effective_marginal_tax_rate": round(emtr, 4),
            "participation_tax_rate": round(participation_tax_rate, 4),
            "trap_risk": trap_risk,
            "net_gain_per_hour": round(net_gain_per_hour, 2) if net_gain_per_hour else None,
            "breakeven_monthly_hours": round(breakeven_hours, 1) if breakeven_hours and breakeven_hours < 1000 else None,
        }

        # --- Score ---
        score = 25.0

        # Targeting problems
        targeting = results.get("targeting", {})
        if targeting.get("quality") == "poorly-targeted":
            score += 25
        elif targeting.get("quality") == "moderately-targeted":
            score += 10

        excl = targeting.get("exclusion_error")
        if excl is not None and excl > 0.5:
            score += 15

        # Inadequate transfers
        if results.get("adequacy", {}).get("assessment") == "inadequate":
            score += 15
        elif results.get("adequacy", {}).get("assessment") == "moderate":
            score += 5

        # High EMTR
        if emtr > 0.7:
            score += 15
        elif emtr > 0.5:
            score += 5

        score = max(0.0, min(100.0, score))

        return {"score": round(score, 1), "results": results}
