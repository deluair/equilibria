from app.layers.environmental.biodiversity_economics import BiodiversityEconomics
from app.layers.environmental.carbon_pricing import CarbonPricing
from app.layers.environmental.climate_damage import ClimateDamage
from app.layers.environmental.ekc import EnvironmentalKuznetsCurve
from app.layers.environmental.green_growth import GreenGrowth
from app.layers.environmental.pollution_haven import PollutionHaven
from app.layers.environmental.renewable_transition import RenewableTransition
from app.layers.environmental.water_economics import WaterEconomics

ALL_MODULES = [
    CarbonPricing,
    PollutionHaven,
    EnvironmentalKuznetsCurve,
    GreenGrowth,
    RenewableTransition,
    ClimateDamage,
    BiodiversityEconomics,
    WaterEconomics,
]

__all__ = [
    "CarbonPricing",
    "PollutionHaven",
    "EnvironmentalKuznetsCurve",
    "GreenGrowth",
    "RenewableTransition",
    "ClimateDamage",
    "BiodiversityEconomics",
    "WaterEconomics",
    "ALL_MODULES",
]
