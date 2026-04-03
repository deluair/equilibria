"""Unit tests for crosscutting layer modules."""

import pytest

VALID_SIGNALS = {"UNAVAILABLE", "STABLE", "WATCH", "STRESS", "CRISIS"}


# ---------------------------------------------------------------------------
# MacroTradeNexus
# ---------------------------------------------------------------------------


def test_macro_trade_nexus_instantiation():
    from app.layers.crosscutting.macro_trade_nexus import MacroTradeNexus
    assert MacroTradeNexus() is not None


def test_macro_trade_nexus_layer_id():
    from app.layers.crosscutting.macro_trade_nexus import MacroTradeNexus
    assert MacroTradeNexus().layer_id == "lCX"


async def test_macro_trade_nexus_compute_empty_db_returns_dict(db_conn):
    from app.layers.crosscutting.macro_trade_nexus import MacroTradeNexus
    result = await MacroTradeNexus().compute(db_conn, country_iso3="USA")
    assert isinstance(result, dict)


async def test_macro_trade_nexus_compute_empty_db_score_is_none(db_conn):
    from app.layers.crosscutting.macro_trade_nexus import MacroTradeNexus
    result = await MacroTradeNexus().compute(db_conn, country_iso3="USA")
    assert result.get("score") is None


async def test_macro_trade_nexus_run_valid_signal(db_conn):
    from app.layers.crosscutting.macro_trade_nexus import MacroTradeNexus
    result = await MacroTradeNexus().run(db_conn, country_iso3="USA")
    assert result["signal"] in VALID_SIGNALS


async def test_macro_trade_nexus_run_has_layer_id(db_conn):
    from app.layers.crosscutting.macro_trade_nexus import MacroTradeNexus
    result = await MacroTradeNexus().run(db_conn, country_iso3="USA")
    assert result["layer_id"] == "lCX"


# ---------------------------------------------------------------------------
# FinanceDevelopmentNexus
# ---------------------------------------------------------------------------


def test_finance_development_nexus_instantiation():
    from app.layers.crosscutting.finance_development_nexus import FinanceDevelopmentNexus
    assert FinanceDevelopmentNexus() is not None


def test_finance_development_nexus_layer_id():
    from app.layers.crosscutting.finance_development_nexus import FinanceDevelopmentNexus
    assert FinanceDevelopmentNexus().layer_id == "lCX"


async def test_finance_development_nexus_compute_empty_db_returns_dict(db_conn):
    from app.layers.crosscutting.finance_development_nexus import FinanceDevelopmentNexus
    result = await FinanceDevelopmentNexus().compute(db_conn, country_iso3="DEU")
    assert isinstance(result, dict)


async def test_finance_development_nexus_compute_empty_db_score_is_none(db_conn):
    from app.layers.crosscutting.finance_development_nexus import FinanceDevelopmentNexus
    result = await FinanceDevelopmentNexus().compute(db_conn, country_iso3="DEU")
    assert result.get("score") is None


async def test_finance_development_nexus_run_valid_signal(db_conn):
    from app.layers.crosscutting.finance_development_nexus import FinanceDevelopmentNexus
    result = await FinanceDevelopmentNexus().run(db_conn, country_iso3="DEU")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# EnergyMacroLinkage
# ---------------------------------------------------------------------------


def test_energy_macro_linkage_instantiation():
    from app.layers.crosscutting.energy_macro_linkage import EnergyMacroLinkage
    assert EnergyMacroLinkage() is not None


def test_energy_macro_linkage_layer_id():
    from app.layers.crosscutting.energy_macro_linkage import EnergyMacroLinkage
    assert EnergyMacroLinkage().layer_id == "lCX"


async def test_energy_macro_linkage_compute_empty_db_returns_dict(db_conn):
    from app.layers.crosscutting.energy_macro_linkage import EnergyMacroLinkage
    result = await EnergyMacroLinkage().compute(db_conn, country_iso3="JPN")
    assert isinstance(result, dict)


async def test_energy_macro_linkage_compute_empty_db_score_is_none(db_conn):
    from app.layers.crosscutting.energy_macro_linkage import EnergyMacroLinkage
    result = await EnergyMacroLinkage().compute(db_conn, country_iso3="JPN")
    assert result.get("score") is None


async def test_energy_macro_linkage_run_valid_signal(db_conn):
    from app.layers.crosscutting.energy_macro_linkage import EnergyMacroLinkage
    result = await EnergyMacroLinkage().run(db_conn, country_iso3="JPN")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# ClimateEconomicImpact
# ---------------------------------------------------------------------------


def test_climate_economic_impact_instantiation():
    from app.layers.crosscutting.climate_economic_impact import ClimateEconomicImpact
    assert ClimateEconomicImpact() is not None


def test_climate_economic_impact_layer_id():
    from app.layers.crosscutting.climate_economic_impact import ClimateEconomicImpact
    assert ClimateEconomicImpact().layer_id == "lCX"


async def test_climate_economic_impact_compute_empty_db_returns_dict(db_conn):
    from app.layers.crosscutting.climate_economic_impact import ClimateEconomicImpact
    result = await ClimateEconomicImpact().compute(db_conn, country_iso3="BGD")
    assert isinstance(result, dict)


async def test_climate_economic_impact_compute_empty_db_score_is_none(db_conn):
    from app.layers.crosscutting.climate_economic_impact import ClimateEconomicImpact
    result = await ClimateEconomicImpact().compute(db_conn, country_iso3="BGD")
    assert result.get("score") is None


async def test_climate_economic_impact_run_valid_signal(db_conn):
    from app.layers.crosscutting.climate_economic_impact import ClimateEconomicImpact
    result = await ClimateEconomicImpact().run(db_conn, country_iso3="BGD")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# HealthLaborProductivity
# ---------------------------------------------------------------------------


def test_health_labor_productivity_instantiation():
    from app.layers.crosscutting.health_labor_productivity import HealthLaborProductivity
    assert HealthLaborProductivity() is not None


def test_health_labor_productivity_layer_id():
    from app.layers.crosscutting.health_labor_productivity import HealthLaborProductivity
    assert HealthLaborProductivity().layer_id == "lCX"


async def test_health_labor_productivity_compute_empty_db_returns_dict(db_conn):
    from app.layers.crosscutting.health_labor_productivity import HealthLaborProductivity
    result = await HealthLaborProductivity().compute(db_conn, country_iso3="IND")
    assert isinstance(result, dict)


async def test_health_labor_productivity_compute_empty_db_score_is_none(db_conn):
    from app.layers.crosscutting.health_labor_productivity import HealthLaborProductivity
    result = await HealthLaborProductivity().compute(db_conn, country_iso3="IND")
    assert result.get("score") is None


async def test_health_labor_productivity_run_valid_signal(db_conn):
    from app.layers.crosscutting.health_labor_productivity import HealthLaborProductivity
    result = await HealthLaborProductivity().run(db_conn, country_iso3="IND")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# InstitutionalQualityGrowth
# ---------------------------------------------------------------------------


def test_institutional_quality_growth_instantiation():
    from app.layers.crosscutting.institutional_quality_growth import InstitutionalQualityGrowth
    assert InstitutionalQualityGrowth() is not None


def test_institutional_quality_growth_layer_id():
    from app.layers.crosscutting.institutional_quality_growth import InstitutionalQualityGrowth
    assert InstitutionalQualityGrowth().layer_id == "lCX"


async def test_institutional_quality_growth_compute_empty_db_returns_dict(db_conn):
    from app.layers.crosscutting.institutional_quality_growth import InstitutionalQualityGrowth
    result = await InstitutionalQualityGrowth().compute(db_conn, country_iso3="NGA")
    assert isinstance(result, dict)


async def test_institutional_quality_growth_compute_empty_db_score_is_none(db_conn):
    from app.layers.crosscutting.institutional_quality_growth import InstitutionalQualityGrowth
    result = await InstitutionalQualityGrowth().compute(db_conn, country_iso3="NGA")
    assert result.get("score") is None


async def test_institutional_quality_growth_run_valid_signal(db_conn):
    from app.layers.crosscutting.institutional_quality_growth import InstitutionalQualityGrowth
    result = await InstitutionalQualityGrowth().run(db_conn, country_iso3="NGA")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# ConflictEconomicCost
# ---------------------------------------------------------------------------


def test_conflict_economic_cost_instantiation():
    from app.layers.crosscutting.conflict_economic_cost import ConflictEconomicCost
    assert ConflictEconomicCost() is not None


def test_conflict_economic_cost_layer_id():
    from app.layers.crosscutting.conflict_economic_cost import ConflictEconomicCost
    assert ConflictEconomicCost().layer_id == "lCX"


async def test_conflict_economic_cost_compute_empty_db_returns_dict(db_conn):
    from app.layers.crosscutting.conflict_economic_cost import ConflictEconomicCost
    result = await ConflictEconomicCost().compute(db_conn, country_iso3="SYR")
    assert isinstance(result, dict)


async def test_conflict_economic_cost_compute_empty_db_score_is_none(db_conn):
    from app.layers.crosscutting.conflict_economic_cost import ConflictEconomicCost
    result = await ConflictEconomicCost().compute(db_conn, country_iso3="SYR")
    assert result.get("score") is None


async def test_conflict_economic_cost_run_valid_signal(db_conn):
    from app.layers.crosscutting.conflict_economic_cost import ConflictEconomicCost
    result = await ConflictEconomicCost().run(db_conn, country_iso3="SYR")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# TechnologyInequality
# ---------------------------------------------------------------------------


def test_technology_inequality_instantiation():
    from app.layers.crosscutting.technology_inequality import TechnologyInequality
    assert TechnologyInequality() is not None


def test_technology_inequality_layer_id():
    from app.layers.crosscutting.technology_inequality import TechnologyInequality
    assert TechnologyInequality().layer_id == "lCX"


async def test_technology_inequality_compute_empty_db_returns_dict(db_conn):
    from app.layers.crosscutting.technology_inequality import TechnologyInequality
    result = await TechnologyInequality().compute(db_conn, country_iso3="BRA")
    assert isinstance(result, dict)


async def test_technology_inequality_compute_empty_db_score_is_none(db_conn):
    from app.layers.crosscutting.technology_inequality import TechnologyInequality
    result = await TechnologyInequality().compute(db_conn, country_iso3="BRA")
    assert result.get("score") is None


async def test_technology_inequality_run_valid_signal(db_conn):
    from app.layers.crosscutting.technology_inequality import TechnologyInequality
    result = await TechnologyInequality().run(db_conn, country_iso3="BRA")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# GenderGrowthNexus
# ---------------------------------------------------------------------------


def test_gender_growth_nexus_instantiation():
    from app.layers.crosscutting.gender_growth_nexus import GenderGrowthNexus
    assert GenderGrowthNexus() is not None


def test_gender_growth_nexus_layer_id():
    from app.layers.crosscutting.gender_growth_nexus import GenderGrowthNexus
    assert GenderGrowthNexus().layer_id == "lCX"


async def test_gender_growth_nexus_compute_empty_db_returns_dict(db_conn):
    from app.layers.crosscutting.gender_growth_nexus import GenderGrowthNexus
    result = await GenderGrowthNexus().compute(db_conn, country_iso3="PAK")
    assert isinstance(result, dict)


async def test_gender_growth_nexus_compute_empty_db_score_is_none(db_conn):
    from app.layers.crosscutting.gender_growth_nexus import GenderGrowthNexus
    result = await GenderGrowthNexus().compute(db_conn, country_iso3="PAK")
    assert result.get("score") is None


async def test_gender_growth_nexus_run_valid_signal(db_conn):
    from app.layers.crosscutting.gender_growth_nexus import GenderGrowthNexus
    result = await GenderGrowthNexus().run(db_conn, country_iso3="PAK")
    assert result["signal"] in VALID_SIGNALS


# ---------------------------------------------------------------------------
# MigrationDevelopment
# ---------------------------------------------------------------------------


def test_migration_development_instantiation():
    from app.layers.crosscutting.migration_development import MigrationDevelopment
    assert MigrationDevelopment() is not None


def test_migration_development_layer_id():
    from app.layers.crosscutting.migration_development import MigrationDevelopment
    assert MigrationDevelopment().layer_id == "lCX"


async def test_migration_development_compute_empty_db_returns_dict(db_conn):
    from app.layers.crosscutting.migration_development import MigrationDevelopment
    result = await MigrationDevelopment().compute(db_conn, country_iso3="MEX")
    assert isinstance(result, dict)


async def test_migration_development_compute_empty_db_score_is_none(db_conn):
    from app.layers.crosscutting.migration_development import MigrationDevelopment
    result = await MigrationDevelopment().compute(db_conn, country_iso3="MEX")
    assert result.get("score") is None


async def test_migration_development_run_valid_signal(db_conn):
    from app.layers.crosscutting.migration_development import MigrationDevelopment
    result = await MigrationDevelopment().run(db_conn, country_iso3="MEX")
    assert result["signal"] in VALID_SIGNALS
