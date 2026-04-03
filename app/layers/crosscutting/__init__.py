from app.layers.crosscutting.climate_economic_impact import ClimateEconomicImpact
from app.layers.crosscutting.conflict_economic_cost import ConflictEconomicCost
from app.layers.crosscutting.energy_macro_linkage import EnergyMacroLinkage
from app.layers.crosscutting.finance_development_nexus import FinanceDevelopmentNexus
from app.layers.crosscutting.gender_growth_nexus import GenderGrowthNexus
from app.layers.crosscutting.health_labor_productivity import HealthLaborProductivity
from app.layers.crosscutting.institutional_quality_growth import InstitutionalQualityGrowth
from app.layers.crosscutting.macro_trade_nexus import MacroTradeNexus
from app.layers.crosscutting.migration_development import MigrationDevelopment
from app.layers.crosscutting.technology_inequality import TechnologyInequality

ALL_MODULES = [
    MacroTradeNexus,
    FinanceDevelopmentNexus,
    EnergyMacroLinkage,
    ClimateEconomicImpact,
    HealthLaborProductivity,
    InstitutionalQualityGrowth,
    ConflictEconomicCost,
    TechnologyInequality,
    GenderGrowthNexus,
    MigrationDevelopment,
]

__all__ = [
    "MacroTradeNexus",
    "FinanceDevelopmentNexus",
    "EnergyMacroLinkage",
    "ClimateEconomicImpact",
    "HealthLaborProductivity",
    "InstitutionalQualityGrowth",
    "ConflictEconomicCost",
    "TechnologyInequality",
    "GenderGrowthNexus",
    "MigrationDevelopment",
    "ALL_MODULES",
]
