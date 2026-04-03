"""Circular Economy layer (lCE) — 10 modules."""

from app.layers.circular_economy.biological_cycle_health import BiologicalCycleHealth
from app.layers.circular_economy.circular_business_models import CircularBusinessModels
from app.layers.circular_economy.industrial_symbiosis_index import IndustrialSymbiosisIndex
from app.layers.circular_economy.material_productivity import MaterialProductivity
from app.layers.circular_economy.product_lifetime_index import ProductLifetimeIndex
from app.layers.circular_economy.recycling_rate_gap import RecyclingRateGap
from app.layers.circular_economy.repair_reuse_economy import RepairReuseEconomy
from app.layers.circular_economy.resource_efficiency_trend import ResourceEfficiencyTrend
from app.layers.circular_economy.waste_generation_intensity import WasteGenerationIntensity
from app.layers.circular_economy.waste_trade_dependency import WasteTradeDependency

ALL_MODULES = [
    MaterialProductivity,
    WasteGenerationIntensity,
    RecyclingRateGap,
    ProductLifetimeIndex,
    ResourceEfficiencyTrend,
    RepairReuseEconomy,
    IndustrialSymbiosisIndex,
    BiologicalCycleHealth,
    CircularBusinessModels,
    WasteTradeDependency,
]

__all__ = [
    "MaterialProductivity",
    "WasteGenerationIntensity",
    "RecyclingRateGap",
    "ProductLifetimeIndex",
    "ResourceEfficiencyTrend",
    "RepairReuseEconomy",
    "IndustrialSymbiosisIndex",
    "BiologicalCycleHealth",
    "CircularBusinessModels",
    "WasteTradeDependency",
    "ALL_MODULES",
]
