from app.layers.natural_resources.resource_rent_share import ResourceRentShare
from app.layers.natural_resources.resource_curse_index import ResourceCurseIndex
from app.layers.natural_resources.extractive_sector_linkage import ExtractiveSectorLinkage
from app.layers.natural_resources.depletion_adjusted_savings import DepletionAdjustedSavings
from app.layers.natural_resources.mineral_wealth_per_capita import MineralWealthPerCapita
from app.layers.natural_resources.forest_capital_value import ForestCapitalValue
from app.layers.natural_resources.fisheries_sustainability import FisheriesSustainability
from app.layers.natural_resources.water_resource_stress import WaterResourceStress
from app.layers.natural_resources.resource_revenue_management import ResourceRevenueManagement
from app.layers.natural_resources.biodiversity_economic_value import BiodiversityEconomicValue

ALL_MODULES = [
    ResourceRentShare,
    ResourceCurseIndex,
    ExtractiveSectorLinkage,
    DepletionAdjustedSavings,
    MineralWealthPerCapita,
    ForestCapitalValue,
    FisheriesSustainability,
    WaterResourceStress,
    ResourceRevenueManagement,
    BiodiversityEconomicValue,
]

__all__ = [
    "ResourceRentShare",
    "ResourceCurseIndex",
    "ExtractiveSectorLinkage",
    "DepletionAdjustedSavings",
    "MineralWealthPerCapita",
    "ForestCapitalValue",
    "FisheriesSustainability",
    "WaterResourceStress",
    "ResourceRevenueManagement",
    "BiodiversityEconomicValue",
    "ALL_MODULES",
]
