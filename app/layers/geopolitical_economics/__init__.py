from app.layers.geopolitical_economics.sanctions_economic_cost import SanctionsEconomicCost
from app.layers.geopolitical_economics.economic_statecraft_index import EconomicStatecraftIndex
from app.layers.geopolitical_economics.great_power_competition import GreatPowerCompetition
from app.layers.geopolitical_economics.resource_geopolitics import ResourceGeopoliticsRisk
from app.layers.geopolitical_economics.deglobalization_risk import DeglobalizationRisk
from app.layers.geopolitical_economics.ally_trade_dependence import AllyTradeDependence
from app.layers.geopolitical_economics.tech_decoupling_exposure import TechDecouplingExposure
from app.layers.geopolitical_economics.financial_sanctions_vulnerability import FinancialSanctionsVulnerability
from app.layers.geopolitical_economics.supply_chain_geopolitics import SupplyChainGeopolitics
from app.layers.geopolitical_economics.diplomatic_economic_leverage import DiplomaticEconomicLeverage

ALL_MODULES = [
    SanctionsEconomicCost,
    EconomicStatecraftIndex,
    GreatPowerCompetition,
    ResourceGeopoliticsRisk,
    DeglobalizationRisk,
    AllyTradeDependence,
    TechDecouplingExposure,
    FinancialSanctionsVulnerability,
    SupplyChainGeopolitics,
    DiplomaticEconomicLeverage,
]

__all__ = [
    "SanctionsEconomicCost",
    "EconomicStatecraftIndex",
    "GreatPowerCompetition",
    "ResourceGeopoliticsRisk",
    "DeglobalizationRisk",
    "AllyTradeDependence",
    "TechDecouplingExposure",
    "FinancialSanctionsVulnerability",
    "SupplyChainGeopolitics",
    "DiplomaticEconomicLeverage",
    "ALL_MODULES",
]
