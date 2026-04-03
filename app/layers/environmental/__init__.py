from app.layers.environmental.air_quality import AirQuality
from app.layers.environmental.biodiversity_economics import BiodiversityEconomics
from app.layers.environmental.carbon_pricing import CarbonPricing
from app.layers.environmental.circular_economy import CircularEconomy
from app.layers.environmental.climate_damage import ClimateDamage
from app.layers.environmental.ekc import EnvironmentalKuznetsCurve
from app.layers.environmental.green_growth import GreenGrowth
from app.layers.environmental.ocean_economics import OceanEconomics
from app.layers.environmental.pollution_haven import PollutionHaven
from app.layers.environmental.renewable_transition import RenewableTransition
from app.layers.environmental.urban_heat import UrbanHeat
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
    CircularEconomy,
    OceanEconomics,
    UrbanHeat,
    AirQuality,
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
    "CircularEconomy",
    "OceanEconomics",
    "UrbanHeat",
    "AirQuality",
    "ALL_MODULES",
]
