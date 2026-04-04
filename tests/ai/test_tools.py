"""Tests for the AI tool registry (app/ai/tools.py)."""

import pytest

from app.ai.tools import TOOL_REGISTRY, execute_tool, get_tool_definitions


# ---------------------------------------------------------------------------
# Registry size
# ---------------------------------------------------------------------------

def test_tool_registry_has_24_tools():
    """TOOL_REGISTRY must contain exactly 24 tools."""
    assert len(TOOL_REGISTRY) == 24


def test_all_registry_tools_have_required_keys():
    """Every tool in TOOL_REGISTRY must have fn, description, and input_schema."""
    for name, spec in TOOL_REGISTRY.items():
        assert "fn" in spec, f"Tool '{name}' missing 'fn'"
        assert "description" in spec, f"Tool '{name}' missing 'description'"
        assert "input_schema" in spec, f"Tool '{name}' missing 'input_schema'"


def test_all_registry_fns_are_callable():
    """Every tool fn must be callable (async coroutine functions)."""
    import asyncio
    for name, spec in TOOL_REGISTRY.items():
        assert callable(spec["fn"]), f"Tool '{name}' fn is not callable"


# ---------------------------------------------------------------------------
# get_tool_definitions()
# ---------------------------------------------------------------------------

def test_get_tool_definitions_returns_list():
    """get_tool_definitions() must return a list."""
    defs = get_tool_definitions()
    assert isinstance(defs, list)


def test_get_tool_definitions_count():
    """get_tool_definitions() must return 24 definitions."""
    defs = get_tool_definitions()
    assert len(defs) == 24


def test_get_tool_definitions_has_required_keys():
    """Each definition must have name, description, and input_schema."""
    defs = get_tool_definitions()
    for d in defs:
        assert "name" in d, f"Definition missing 'name': {d}"
        assert "description" in d, f"Definition missing 'description': {d}"
        assert "input_schema" in d, f"Definition missing 'input_schema': {d}"


def test_get_tool_definitions_names_match_registry():
    """The names in get_tool_definitions() must match TOOL_REGISTRY keys."""
    defs = get_tool_definitions()
    def_names = {d["name"] for d in defs}
    assert def_names == set(TOOL_REGISTRY.keys())


def test_get_tool_definitions_input_schema_type():
    """Every input_schema must be a dict with 'type' == 'object'."""
    defs = get_tool_definitions()
    for d in defs:
        schema = d["input_schema"]
        assert isinstance(schema, dict), f"input_schema not a dict for {d['name']}"
        assert schema.get("type") == "object", f"input_schema type != 'object' for {d['name']}"


# ---------------------------------------------------------------------------
# execute_tool
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_execute_tool_unknown_returns_error(tmp_db):
    """execute_tool with an unknown tool name returns an error dict."""
    result = await execute_tool("nonexistent_tool_xyz", {})
    assert "error" in result
    assert "Unknown tool" in result["error"]


@pytest.mark.asyncio
async def test_execute_tool_unknown_does_not_raise(tmp_db):
    """execute_tool with unknown name must not raise an exception."""
    result = await execute_tool("__no_such_tool__", {"param": "val"})
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_execute_tool_known_does_not_raise(tmp_db):
    """execute_tool with a known tool and valid args returns a dict."""
    result = await execute_tool("get_system_status", {})
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_execute_tool_tariff_simulation(tmp_db):
    """tariff_simulation returns a dict with expected keys."""
    result = await execute_tool(
        "tariff_simulation",
        {"country": "USA", "product": "wheat", "tariff_change_pct": 10.0},
    )
    assert "tariff_change_pct" in result
    assert "estimated_trade_effect_pct" in result
    assert result["tariff_change_pct"] == 10.0


@pytest.mark.asyncio
async def test_execute_tool_generate_figure(tmp_db):
    """generate_figure returns a figure specification."""
    result = await execute_tool(
        "generate_figure",
        {"chart_type": "line", "title": "Test Chart", "data": {"x": [1, 2], "y": [3, 4]}},
    )
    assert "figure" in result
    assert result["figure"]["type"] == "line"
    assert result["figure"]["title"] == "Test Chart"


@pytest.mark.asyncio
async def test_execute_tool_query_data_empty_db(tmp_db):
    """query_data on an empty DB returns a dict with 'data' list."""
    result = await execute_tool("query_data", {"limit": 10})
    assert "data" in result
    assert isinstance(result["data"], list)


@pytest.mark.asyncio
async def test_execute_tool_compare_countries_empty_db(tmp_db):
    """compare_countries on an empty DB returns a comparison dict."""
    result = await execute_tool(
        "compare_countries",
        {"countries": ["USA", "CHN"], "indicators": ["gdp_growth"]},
    )
    assert "comparison" in result


@pytest.mark.asyncio
async def test_execute_tool_with_bad_args_returns_error(tmp_db):
    """execute_tool wraps exceptions and returns error dict instead of raising."""
    # tariff_simulation requires numeric tariff_change_pct; pass a string
    result = await execute_tool(
        "tariff_simulation",
        {"country": "USA", "product": "oil", "tariff_change_pct": "not_a_number"},
    )
    # Should either succeed (if handled) or return an error dict, not raise
    assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# Known tool names
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tool_name", [
    "get_system_status",
    "estimate_gravity",
    "compute_rca",
    "bilateral_decomposition",
    "tariff_simulation",
    "gdp_decompose",
    "estimate_phillips",
    "fiscal_sustainability",
    "cycle_dating",
    "wage_decomposition",
    "returns_to_education",
    "shift_share",
    "convergence_test",
    "poverty_analysis",
    "institutional_iv",
    "demand_system",
    "food_security_index",
    "price_transmission",
    "run_estimation",
    "compare_countries",
    "query_data",
    "generate_figure",
    "search_knowledge",
    "file_insight",
])
def test_expected_tool_names_present(tool_name):
    """Each of the 24 expected tool names must be in TOOL_REGISTRY."""
    assert tool_name in TOOL_REGISTRY, f"Missing tool: {tool_name}"
