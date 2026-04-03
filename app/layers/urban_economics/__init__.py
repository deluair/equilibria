from app.layers.urban_economics.city_fiscal_capacity import CityFiscalCapacity
from app.layers.urban_economics.congestion_costs import CongestionCosts
from app.layers.urban_economics.housing_urban_stress import HousingUrbanStress
from app.layers.urban_economics.slum_population import SlumPopulation
from app.layers.urban_economics.urban_climate_vulnerability import UrbanClimateVulnerability
from app.layers.urban_economics.urban_economic_productivity import UrbanEconomicProductivity
from app.layers.urban_economics.urban_heat_island import UrbanHeatIsland
from app.layers.urban_economics.urban_infrastructure_gap import UrbanInfrastructureGap
from app.layers.urban_economics.urban_poverty_rate import UrbanPovertyRate
from app.layers.urban_economics.urbanization_rate import UrbanizationRate

ALL_MODULES = [
    UrbanizationRate,
    UrbanPovertyRate,
    SlumPopulation,
    UrbanInfrastructureGap,
    CongestionCosts,
    UrbanHeatIsland,
    HousingUrbanStress,
    CityFiscalCapacity,
    UrbanEconomicProductivity,
    UrbanClimateVulnerability,
]

__all__ = [
    "UrbanizationRate",
    "UrbanPovertyRate",
    "SlumPopulation",
    "UrbanInfrastructureGap",
    "CongestionCosts",
    "UrbanHeatIsland",
    "HousingUrbanStress",
    "CityFiscalCapacity",
    "UrbanEconomicProductivity",
    "UrbanClimateVulnerability",
    "ALL_MODULES",
]
