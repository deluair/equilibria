from app.layers.commodity_economics.commodity_price_volatility import CommodityPriceVolatility
from app.layers.commodity_economics.terms_of_trade_commodity import TermsOfTradeCommodity
from app.layers.commodity_economics.dutch_disease_risk import DutchDiseaseRisk
from app.layers.commodity_economics.commodity_export_concentration import CommodityExportConcentration
from app.layers.commodity_economics.resource_revenue_dependence import ResourceRevenueDependence
from app.layers.commodity_economics.commodity_cycle_synchrony import CommodityCycleSynchrony
from app.layers.commodity_economics.food_commodity_stress import FoodCommodityStress
from app.layers.commodity_economics.energy_commodity_exposure import EnergyCommodityExposure
from app.layers.commodity_economics.metal_mineral_dependence import MetalMineralDependence
from app.layers.commodity_economics.commodity_fund_adequacy import CommodityFundAdequacy

ALL_MODULES = [
    CommodityPriceVolatility,
    TermsOfTradeCommodity,
    DutchDiseaseRisk,
    CommodityExportConcentration,
    ResourceRevenueDependence,
    CommodityCycleSynchrony,
    FoodCommodityStress,
    EnergyCommodityExposure,
    MetalMineralDependence,
    CommodityFundAdequacy,
]

__all__ = [
    "CommodityPriceVolatility",
    "TermsOfTradeCommodity",
    "DutchDiseaseRisk",
    "CommodityExportConcentration",
    "ResourceRevenueDependence",
    "CommodityCycleSynchrony",
    "FoodCommodityStress",
    "EnergyCommodityExposure",
    "MetalMineralDependence",
    "CommodityFundAdequacy",
    "ALL_MODULES",
]
