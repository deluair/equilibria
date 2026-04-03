from app.layers.agricultural.adaptation_cba import AdaptationCBA
from app.layers.agricultural.ag_competitiveness import AgriculturalCompetitiveness
from app.layers.agricultural.agricultural_distortions import AgriculturalDistortions
from app.layers.agricultural.agricultural_land_productivity import AgriculturalLandProductivity
from app.layers.agricultural.aquaculture import Aquaculture
from app.layers.agricultural.caloric_trade import CaloricTradeBalance
from app.layers.agricultural.climate_ag_vulnerability import ClimateAgVulnerability
from app.layers.agricultural.climate_yield import ClimateYield
from app.layers.agricultural.crop_production_index import CropProductionIndex
from app.layers.agricultural.deforestation_trade import DeforestationTradeNexus
from app.layers.agricultural.demand_system import DemandSystem
from app.layers.agricultural.farm_size import FarmSizeProductivity
from app.layers.agricultural.fertilizer_intensity import FertilizerIntensity
from app.layers.agricultural.fertilizer_response import FertilizerResponse
from app.layers.agricultural.food_import_dependency import FoodImportDependency
from app.layers.agricultural.food_price_volatility import FoodPriceVolatility
from app.layers.agricultural.food_security import FoodSecurityIndex
from app.layers.agricultural.food_waste import FoodWaste
from app.layers.agricultural.irrigation_coverage import IrrigationCoverage
from app.layers.agricultural.irrigation_returns import IrrigationReturns
from app.layers.agricultural.land_use import LandUseChange
from app.layers.agricultural.livestock_production import LivestockProduction
from app.layers.agricultural.market_integration import MarketIntegration
from app.layers.agricultural.organic_transition import OrganicTransition
from app.layers.agricultural.precision_agriculture import PrecisionAgriculture
from app.layers.agricultural.price_transmission import PriceTransmission
from app.layers.agricultural.rural_poverty import RuralPoverty
from app.layers.agricultural.supply_chain_disruption import SupplyChainDisruption
from app.layers.agricultural.supply_elasticity import SupplyElasticity
from app.layers.agricultural.wef_nexus import WEFNexus

ALL_MODULES = [
    SupplyElasticity,
    DemandSystem,
    FoodSecurityIndex,
    FoodPriceVolatility,
    PriceTransmission,
    ClimateYield,
    FertilizerResponse,
    IrrigationReturns,
    FarmSizeProductivity,
    LandUseChange,
    DeforestationTradeNexus,
    CaloricTradeBalance,
    WEFNexus,
    AdaptationCBA,
    SupplyChainDisruption,
    MarketIntegration,
    AgriculturalCompetitiveness,
    AgriculturalDistortions,
    PrecisionAgriculture,
    Aquaculture,
    FoodWaste,
    OrganicTransition,
    AgriculturalLandProductivity,
    FoodImportDependency,
    RuralPoverty,
    IrrigationCoverage,
    FertilizerIntensity,
    CropProductionIndex,
    LivestockProduction,
    ClimateAgVulnerability,
]

__all__ = [
    "SupplyElasticity",
    "DemandSystem",
    "FoodSecurityIndex",
    "FoodPriceVolatility",
    "PriceTransmission",
    "ClimateYield",
    "FertilizerResponse",
    "IrrigationReturns",
    "FarmSizeProductivity",
    "LandUseChange",
    "DeforestationTradeNexus",
    "CaloricTradeBalance",
    "WEFNexus",
    "AdaptationCBA",
    "SupplyChainDisruption",
    "MarketIntegration",
    "AgriculturalCompetitiveness",
    "AgriculturalDistortions",
    "PrecisionAgriculture",
    "Aquaculture",
    "FoodWaste",
    "OrganicTransition",
    "AgriculturalLandProductivity",
    "FoodImportDependency",
    "RuralPoverty",
    "IrrigationCoverage",
    "FertilizerIntensity",
    "CropProductionIndex",
    "LivestockProduction",
    "ClimateAgVulnerability",
    "ALL_MODULES",
]
