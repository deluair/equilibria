from app.layers.sustainability.sdg_progress import SDGProgress
from app.layers.sustainability.green_growth_index import GreenGrowthIndex
from app.layers.sustainability.circular_economy import CircularEconomy
from app.layers.sustainability.renewable_energy_transition import RenewableEnergyTransition
from app.layers.sustainability.carbon_footprint import CarbonFootprint
from app.layers.sustainability.biodiversity_pressure import BiodiversityPressure
from app.layers.sustainability.water_sustainability import WaterSustainability
from app.layers.sustainability.land_degradation import LandDegradation
from app.layers.sustainability.sustainable_consumption import SustainableConsumption
from app.layers.sustainability.environmental_governance import EnvironmentalGovernance
from app.layers.sustainability.climate_resilience import ClimateResilience
from app.layers.sustainability.just_transition_index import JustTransitionIndex

ALL_MODULES = [
    SDGProgress,
    GreenGrowthIndex,
    CircularEconomy,
    RenewableEnergyTransition,
    CarbonFootprint,
    BiodiversityPressure,
    WaterSustainability,
    LandDegradation,
    SustainableConsumption,
    EnvironmentalGovernance,
    ClimateResilience,
    JustTransitionIndex,
]

__all__ = [
    "SDGProgress",
    "GreenGrowthIndex",
    "CircularEconomy",
    "RenewableEnergyTransition",
    "CarbonFootprint",
    "BiodiversityPressure",
    "WaterSustainability",
    "LandDegradation",
    "SustainableConsumption",
    "EnvironmentalGovernance",
    "ClimateResilience",
    "JustTransitionIndex",
    "ALL_MODULES",
]
