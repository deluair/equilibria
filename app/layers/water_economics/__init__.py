"""L-WA Water Economics layer modules."""

from app.layers.water_economics.climate_water_nexus import ClimateWaterNexus
from app.layers.water_economics.groundwater_depletion_rate import GroundwaterDepletionRate
from app.layers.water_economics.irrigation_water_efficiency import IrrigationWaterEfficiency
from app.layers.water_economics.transboundary_water_risk import TransboundaryWaterRisk
from app.layers.water_economics.water_access_gap import WaterAccessGap
from app.layers.water_economics.water_infrastructure_investment import WaterInfrastructureInvestment
from app.layers.water_economics.water_pricing_reform import WaterPricingReform
from app.layers.water_economics.water_productivity import WaterProductivity
from app.layers.water_economics.water_sanitation_economics import WaterSanitationEconomics
from app.layers.water_economics.water_scarcity_index import WaterScarcityIndex

ALL_MODULES = [
    WaterScarcityIndex,
    WaterAccessGap,
    WaterProductivity,
    TransboundaryWaterRisk,
    GroundwaterDepletionRate,
    WaterSanitationEconomics,
    IrrigationWaterEfficiency,
    WaterInfrastructureInvestment,
    WaterPricingReform,
    ClimateWaterNexus,
]

__all__ = [
    "WaterScarcityIndex",
    "WaterAccessGap",
    "WaterProductivity",
    "TransboundaryWaterRisk",
    "GroundwaterDepletionRate",
    "WaterSanitationEconomics",
    "IrrigationWaterEfficiency",
    "WaterInfrastructureInvestment",
    "WaterPricingReform",
    "ClimateWaterNexus",
    "ALL_MODULES",
]
