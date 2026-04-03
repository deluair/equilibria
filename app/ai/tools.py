"""20 structured tools for the Equilibria AI brain."""

from __future__ import annotations

import json
import logging

from app import db

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tool implementations (stubs that query DB and return structured dicts)
# ---------------------------------------------------------------------------


async def get_system_status(layer: str | None = None) -> dict:
    """Composite score across all analytical layers."""
    sql = "SELECT layer, score, signal, created_at FROM analysis_results WHERE analysis_type = 'composite'"
    params: tuple = ()
    if layer:
        sql += " AND layer = ?"
        params = (layer,)
    sql += " ORDER BY created_at DESC LIMIT 6"
    rows = await db.fetch_all(sql, params)
    return {
        "layers": rows or [],
        "composite": sum(r["score"] for r in rows) / max(len(rows), 1) if rows else None,
        "_citation": "Equilibria composite score (CEAS), internal calculation",
    }


async def estimate_gravity(
    reporter: str, partner: str, year: int | None = None
) -> dict:
    """Run gravity model on bilateral trade data."""
    sql = """
        SELECT ar.result, ar.score, ar.created_at
        FROM analysis_results ar
        WHERE ar.analysis_type = 'gravity'
          AND ar.parameters LIKE ? AND ar.parameters LIKE ?
        ORDER BY ar.created_at DESC LIMIT 1
    """
    params = (f"%{reporter}%", f"%{partner}%")
    row = await db.fetch_one(sql, params)
    if row:
        return {
            "result": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "score": row["score"],
            "_citation": f"Gravity model estimation, {reporter}-{partner}, Equilibria",
        }
    return {
        "result": None,
        "message": f"No gravity estimate found for {reporter}-{partner}",
        "_citation": "Gravity model (Anderson-van Wincoop), Equilibria",
    }


async def compute_rca(country: str, product: str, year: int | None = None) -> dict:
    """Compute Revealed Comparative Advantage for a country-product pair."""
    sql = """
        SELECT ar.result, ar.score
        FROM analysis_results ar
        WHERE ar.analysis_type = 'rca'
          AND ar.country_iso3 = ? AND ar.parameters LIKE ?
        ORDER BY ar.created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(), f"%{product}%"))
    if row:
        return {
            "rca": row["score"],
            "detail": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"Balassa RCA, {country}, product {product}, Equilibria",
        }
    return {
        "rca": None,
        "message": f"No RCA data for {country}/{product}",
        "_citation": "Balassa (1965) Revealed Comparative Advantage",
    }


async def bilateral_decomposition(reporter: str, partner: str) -> dict:
    """Decompose bilateral trade into extensive and intensive margins."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'bilateral_decomposition'
          AND parameters LIKE ? AND parameters LIKE ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (f"%{reporter}%", f"%{partner}%"))
    if row:
        return {
            "decomposition": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"Bilateral decomposition, {reporter}-{partner}, Hummels-Klenow method",
        }
    return {"decomposition": None, "message": "No decomposition data available", "_citation": "Hummels-Klenow (2005)"}


async def tariff_simulation(
    country: str, product: str, tariff_change_pct: float
) -> dict:
    """Simulate tariff impact on trade flows."""
    sql = """
        SELECT result FROM analysis_results
        WHERE analysis_type = 'tariff_sim'
          AND country_iso3 = ? AND parameters LIKE ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(), f"%{product}%"))
    base = json.loads(row["result"]) if row else {}
    elasticity = base.get("import_elasticity", -1.5)
    trade_effect_pct = elasticity * tariff_change_pct
    return {
        "country": country,
        "product": product,
        "tariff_change_pct": tariff_change_pct,
        "estimated_trade_effect_pct": round(trade_effect_pct, 2),
        "elasticity_used": elasticity,
        "_citation": f"Tariff simulation, elasticity-based, {country}/{product}",
    }


async def gdp_decompose(country: str, year: int | None = None) -> dict:
    """GDP expenditure decomposition for a country."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'gdp_decomposition' AND country_iso3 = ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(),))
    if row:
        return {
            "decomposition": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"GDP expenditure decomposition, {country}, WDI/IMF WEO",
        }
    return {"decomposition": None, "message": f"No GDP decomposition for {country}", "_citation": "World Bank WDI"}


async def estimate_phillips(country: str, variant: str = "traditional") -> dict:
    """Estimate Phillips curve."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'phillips_curve' AND country_iso3 = ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(),))
    if row:
        return {
            "estimate": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"Phillips curve ({variant}), {country}, FRED/WDI",
        }
    return {"estimate": None, "message": f"No Phillips curve for {country}", "_citation": "Phillips (1958)"}


async def fiscal_sustainability(country: str) -> dict:
    """Debt sustainability analysis (r-g framework)."""
    sql = """
        SELECT result, score, signal FROM analysis_results
        WHERE analysis_type = 'fiscal_sustainability' AND country_iso3 = ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(),))
    if row:
        return {
            "assessment": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "score": row["score"],
            "signal": row["signal"],
            "_citation": f"Debt sustainability (r-g), {country}, IMF WEO",
        }
    return {"assessment": None, "message": f"No fiscal data for {country}", "_citation": "IMF Debt Sustainability Framework"}


async def cycle_dating(country: str) -> dict:
    """Business cycle dating for a country."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'cycle_dating' AND country_iso3 = ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(),))
    if row:
        return {
            "cycles": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"Business cycle dating (HP filter), {country}",
        }
    return {"cycles": None, "message": f"No cycle data for {country}", "_citation": "Hodrick-Prescott / Hamilton filter"}


async def wage_decomposition(country: str, groups: str = "gender") -> dict:
    """Oaxaca-Blinder wage decomposition."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'wage_decomposition' AND country_iso3 = ?
          AND parameters LIKE ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(), f"%{groups}%"))
    if row:
        return {
            "decomposition": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"Oaxaca-Blinder decomposition ({groups}), {country}, ILO/BLS",
        }
    return {"decomposition": None, "message": f"No wage decomposition for {country}", "_citation": "Oaxaca (1973), Blinder (1973)"}


async def returns_to_education(country: str, method: str = "ols") -> dict:
    """Mincer equation estimation."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'returns_education' AND country_iso3 = ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(),))
    if row:
        return {
            "estimate": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"Mincer returns to education ({method}), {country}",
        }
    return {"estimate": None, "message": f"No Mincer estimates for {country}", "_citation": "Mincer (1974)"}


async def shift_share(country: str, shock_variable: str) -> dict:
    """Bartik instrument construction."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'shift_share' AND country_iso3 = ?
          AND parameters LIKE ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(), f"%{shock_variable}%"))
    if row:
        return {
            "instrument": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"Bartik shift-share IV, {country}, {shock_variable}",
        }
    return {"instrument": None, "message": "No shift-share data", "_citation": "Bartik (1991), Goldsmith-Pinkham et al. (2020)"}


async def convergence_test(
    countries: list[str], variable: str = "gdp_per_capita", period: str = "1990-2020"
) -> dict:
    """Beta/sigma convergence across countries."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'convergence'
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql)
    if row:
        return {
            "convergence": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"Beta/sigma convergence, {variable}, {period}, PWT/WDI",
        }
    return {"convergence": None, "message": "No convergence results", "_citation": "Barro-Sala-i-Martin convergence framework"}


async def poverty_analysis(country: str) -> dict:
    """Poverty headcount and dynamics."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'poverty' AND country_iso3 = ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(),))
    if row:
        return {
            "poverty": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"Poverty analysis, {country}, PovcalNet/PIP",
        }
    return {"poverty": None, "message": f"No poverty data for {country}", "_citation": "World Bank PovcalNet/PIP"}


async def institutional_iv(country: str) -> dict:
    """IV estimation for institutional quality (settler mortality, legal origins)."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'institutional_iv' AND country_iso3 = ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(),))
    if row:
        return {
            "iv_estimate": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"Institutional quality IV, {country}, Acemoglu et al. (2001)",
        }
    return {"iv_estimate": None, "message": f"No IV estimate for {country}", "_citation": "Acemoglu, Johnson, Robinson (2001)"}


async def demand_system(country: str, system: str = "aids") -> dict:
    """AIDS/EASI demand system estimation."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'demand_system' AND country_iso3 = ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(),))
    if row:
        return {
            "elasticities": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"{system.upper()} demand system, {country}, FAOSTAT",
        }
    return {"elasticities": None, "message": f"No demand system for {country}", "_citation": "Deaton-Muellbauer (1980) AIDS"}


async def food_security_index(country: str) -> dict:
    """Composite food security score."""
    sql = """
        SELECT result, score, signal FROM analysis_results
        WHERE analysis_type = 'food_security' AND country_iso3 = ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (country.upper(),))
    if row:
        return {
            "index": row["score"],
            "components": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "signal": row["signal"],
            "_citation": f"Food security index, {country}, FAOSTAT/USDA",
        }
    return {"index": None, "message": f"No food security data for {country}", "_citation": "FAOSTAT, USDA/ERS"}


async def price_transmission(
    commodity: str, origin_market: str, destination_market: str
) -> dict:
    """Commodity price transmission analysis (VECM)."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = 'price_transmission'
          AND parameters LIKE ? AND parameters LIKE ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (f"%{commodity}%", f"%{origin_market}%"))
    if row:
        return {
            "transmission": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"Price transmission ({commodity}), {origin_market}->{destination_market}, VECM",
        }
    return {"transmission": None, "message": "No transmission data", "_citation": "VECM price transmission, FAOSTAT"}


async def run_estimation(
    method: str,
    dependent: str,
    independents: list[str],
    country: str | None = None,
    controls: list[str] | None = None,
) -> dict:
    """General-purpose estimator (OLS/IV/Panel/DiD/RDD)."""
    sql = """
        SELECT result, score FROM analysis_results
        WHERE analysis_type = ?
        ORDER BY created_at DESC LIMIT 1
    """
    row = await db.fetch_one(sql, (method.lower(),))
    if row:
        return {
            "method": method,
            "estimate": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
            "_citation": f"{method.upper()} estimation, Equilibria estimation engine",
        }
    return {
        "method": method,
        "estimate": None,
        "message": f"No {method} results available. Run the estimation module first.",
        "_citation": f"{method.upper()} estimator, Equilibria",
    }


async def compare_countries(countries: list[str], indicators: list[str]) -> dict:
    """Side-by-side country comparison."""
    results = {}
    for iso3 in countries:
        iso3_upper = iso3.upper()
        rows = await db.fetch_all(
            """
            SELECT ds.name, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
            ORDER BY dp.date DESC
            LIMIT 100
            """,
            (iso3_upper,),
        )
        results[iso3_upper] = rows
    return {
        "comparison": results,
        "_citation": "Cross-country comparison, WDI/FRED/IMF, Equilibria",
    }


async def query_data(
    source: str | None = None,
    country: str | None = None,
    series_name: str | None = None,
    limit: int = 50,
) -> dict:
    """Flexible data query from the database."""
    conditions = []
    params: list = []
    if source:
        conditions.append("ds.source = ?")
        params.append(source)
    if country:
        conditions.append("ds.country_iso3 = ?")
        params.append(country.upper())
    if series_name:
        conditions.append("ds.name LIKE ?")
        params.append(f"%{series_name}%")
    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT ds.source, ds.name, ds.country_iso3, dp.date, dp.value
        FROM data_points dp
        JOIN data_series ds ON dp.series_id = ds.id
        WHERE {where}
        ORDER BY dp.date DESC
        LIMIT ?
    """
    params.append(limit)
    rows = await db.fetch_all(sql, tuple(params))
    return {
        "data": rows,
        "row_count": len(rows),
        "_citation": f"Equilibria database query, source={source or 'all'}",
    }


async def generate_figure(
    chart_type: str,
    title: str,
    data: dict,
) -> dict:
    """Create a Plotly figure specification."""
    fig_spec = {
        "type": chart_type,
        "title": title,
        "data": data,
        "layout": {
            "template": "plotly_white",
            "title": {"text": title},
            "font": {"family": "Source Sans Pro, sans-serif"},
        },
    }
    return {
        "figure": fig_spec,
        "_citation": "Equilibria figure generator",
    }


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

TOOL_REGISTRY: dict[str, dict] = {
    "get_system_status": {
        "fn": get_system_status,
        "description": "Get composite economic analysis score across all layers (trade, macro, labor, development, agricultural). Optionally filter by layer.",
        "input_schema": {
            "type": "object",
            "properties": {
                "layer": {
                    "type": "string",
                    "description": "Optional layer filter: l1 (trade), l2 (macro), l3 (labor), l4 (development), l5 (agricultural)",
                    "enum": ["l1", "l2", "l3", "l4", "l5"],
                },
            },
            "required": [],
        },
    },
    "estimate_gravity": {
        "fn": estimate_gravity,
        "description": "Run gravity model estimation on bilateral trade data between two countries.",
        "input_schema": {
            "type": "object",
            "properties": {
                "reporter": {"type": "string", "description": "ISO3 code of reporting country"},
                "partner": {"type": "string", "description": "ISO3 code of partner country"},
                "year": {"type": "integer", "description": "Optional year for the estimate"},
            },
            "required": ["reporter", "partner"],
        },
    },
    "compute_rca": {
        "fn": compute_rca,
        "description": "Compute Revealed Comparative Advantage (Balassa index) for a country-product pair. RCA > 1 means comparative advantage.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
                "product": {"type": "string", "description": "HS6 product code or name"},
                "year": {"type": "integer", "description": "Optional year"},
            },
            "required": ["country", "product"],
        },
    },
    "bilateral_decomposition": {
        "fn": bilateral_decomposition,
        "description": "Decompose bilateral trade into extensive margin (number of products) and intensive margin (value per product).",
        "input_schema": {
            "type": "object",
            "properties": {
                "reporter": {"type": "string", "description": "ISO3 code of reporter"},
                "partner": {"type": "string", "description": "ISO3 code of partner"},
            },
            "required": ["reporter", "partner"],
        },
    },
    "tariff_simulation": {
        "fn": tariff_simulation,
        "description": "Simulate the impact of a tariff change on trade flows for a country-product pair.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
                "product": {"type": "string", "description": "Product code or name"},
                "tariff_change_pct": {"type": "number", "description": "Tariff change in percentage points (e.g., 10 for +10%)"},
            },
            "required": ["country", "product", "tariff_change_pct"],
        },
    },
    "gdp_decompose": {
        "fn": gdp_decompose,
        "description": "GDP expenditure-side decomposition (consumption, investment, government, net exports) for a country.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
                "year": {"type": "integer", "description": "Optional year"},
            },
            "required": ["country"],
        },
    },
    "estimate_phillips": {
        "fn": estimate_phillips,
        "description": "Estimate the Phillips curve (inflation-unemployment tradeoff) for a country.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
                "variant": {
                    "type": "string",
                    "description": "Phillips curve variant",
                    "enum": ["traditional", "expectations_augmented", "nkpc"],
                },
            },
            "required": ["country"],
        },
    },
    "fiscal_sustainability": {
        "fn": fiscal_sustainability,
        "description": "Debt sustainability analysis using the r-g (interest rate minus growth rate) framework.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
            },
            "required": ["country"],
        },
    },
    "cycle_dating": {
        "fn": cycle_dating,
        "description": "Business cycle dating (peaks and troughs) for a country using HP filter or Hamilton filter.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
            },
            "required": ["country"],
        },
    },
    "wage_decomposition": {
        "fn": wage_decomposition,
        "description": "Oaxaca-Blinder wage decomposition to quantify explained vs unexplained wage gaps between groups.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
                "groups": {
                    "type": "string",
                    "description": "Decomposition dimension",
                    "enum": ["gender", "race", "education"],
                },
            },
            "required": ["country"],
        },
    },
    "returns_to_education": {
        "fn": returns_to_education,
        "description": "Estimate returns to education using the Mincer wage equation.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
                "method": {
                    "type": "string",
                    "description": "Estimation method",
                    "enum": ["ols", "iv"],
                },
            },
            "required": ["country"],
        },
    },
    "shift_share": {
        "fn": shift_share,
        "description": "Construct a Bartik (shift-share) instrument for identifying exogenous labor demand shocks.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
                "shock_variable": {"type": "string", "description": "The shock variable (e.g., 'import_competition', 'technology')"},
            },
            "required": ["country", "shock_variable"],
        },
    },
    "convergence_test": {
        "fn": convergence_test,
        "description": "Test for beta convergence (poorer countries growing faster) and sigma convergence (declining dispersion) across countries.",
        "input_schema": {
            "type": "object",
            "properties": {
                "countries": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of ISO3 country codes",
                },
                "variable": {"type": "string", "description": "Variable to test (default: gdp_per_capita)"},
                "period": {"type": "string", "description": "Period range, e.g. '1990-2020'"},
            },
            "required": ["countries"],
        },
    },
    "poverty_analysis": {
        "fn": poverty_analysis,
        "description": "Poverty headcount ratios, gap, severity, and dynamics for a country.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
            },
            "required": ["country"],
        },
    },
    "institutional_iv": {
        "fn": institutional_iv,
        "description": "IV estimation for institutional quality using settler mortality or legal origins as instruments (Acemoglu et al. 2001).",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
            },
            "required": ["country"],
        },
    },
    "demand_system": {
        "fn": demand_system,
        "description": "Estimate a demand system (AIDS or EASI) for food/agricultural products.",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
                "system": {
                    "type": "string",
                    "description": "Demand system type",
                    "enum": ["aids", "easi"],
                },
            },
            "required": ["country"],
        },
    },
    "food_security_index": {
        "fn": food_security_index,
        "description": "Compute a composite food security index (availability, access, utilization, stability).",
        "input_schema": {
            "type": "object",
            "properties": {
                "country": {"type": "string", "description": "ISO3 country code"},
            },
            "required": ["country"],
        },
    },
    "price_transmission": {
        "fn": price_transmission,
        "description": "Analyze commodity price transmission between markets using VECM and threshold cointegration.",
        "input_schema": {
            "type": "object",
            "properties": {
                "commodity": {"type": "string", "description": "Commodity name (e.g., 'wheat', 'rice', 'oil')"},
                "origin_market": {"type": "string", "description": "Origin market (e.g., 'world', 'USA')"},
                "destination_market": {"type": "string", "description": "Destination market (e.g., 'BGD', 'IND')"},
            },
            "required": ["commodity", "origin_market", "destination_market"],
        },
    },
    "run_estimation": {
        "fn": run_estimation,
        "description": "Run a general-purpose econometric estimation (OLS, IV, Panel FE, DiD, RDD). Use for custom regressions not covered by specialized tools.",
        "input_schema": {
            "type": "object",
            "properties": {
                "method": {
                    "type": "string",
                    "description": "Estimation method",
                    "enum": ["ols", "iv", "panel_fe", "did", "rdd"],
                },
                "dependent": {"type": "string", "description": "Dependent variable name"},
                "independents": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of independent variable names",
                },
                "country": {"type": "string", "description": "Optional ISO3 country filter"},
                "controls": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional control variables",
                },
            },
            "required": ["method", "dependent", "independents"],
        },
    },
    "compare_countries": {
        "fn": compare_countries,
        "description": "Side-by-side comparison of multiple countries across selected indicators.",
        "input_schema": {
            "type": "object",
            "properties": {
                "countries": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of ISO3 country codes to compare",
                },
                "indicators": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Indicator names to compare",
                },
            },
            "required": ["countries", "indicators"],
        },
    },
    "query_data": {
        "fn": query_data,
        "description": "Flexible query against the Equilibria database. Filter by data source, country, or series name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {"type": "string", "description": "Data source (e.g., 'FRED', 'WDI', 'ILO', 'FAOSTAT')"},
                "country": {"type": "string", "description": "ISO3 country code"},
                "series_name": {"type": "string", "description": "Partial match on series name"},
                "limit": {"type": "integer", "description": "Max rows to return (default 50)"},
            },
            "required": [],
        },
    },
    "generate_figure": {
        "fn": generate_figure,
        "description": "Generate a Plotly figure specification for visualization.",
        "input_schema": {
            "type": "object",
            "properties": {
                "chart_type": {
                    "type": "string",
                    "description": "Chart type",
                    "enum": ["line", "bar", "scatter", "heatmap", "choropleth", "treemap"],
                },
                "title": {"type": "string", "description": "Chart title"},
                "data": {"type": "object", "description": "Data payload for the chart (x, y, labels, etc.)"},
            },
            "required": ["chart_type", "title", "data"],
        },
    },
}


def get_tool_definitions() -> list[dict]:
    """Return tool definitions formatted for the Anthropic API."""
    tools = []
    for name, spec in TOOL_REGISTRY.items():
        tools.append({
            "name": name,
            "description": spec["description"],
            "input_schema": spec["input_schema"],
        })
    return tools


async def execute_tool(name: str, arguments: dict) -> dict:
    """Execute a named tool with the given arguments. Returns structured dict."""
    spec = TOOL_REGISTRY.get(name)
    if spec is None:
        return {"error": f"Unknown tool: {name}"}
    try:
        result = await spec["fn"](**arguments)
        return result
    except Exception as e:
        logger.exception("Tool %s failed", name)
        return {"error": str(e), "_citation": f"Tool {name} error"}
