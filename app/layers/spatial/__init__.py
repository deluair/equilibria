from app.layers.spatial.agglomeration import Agglomeration
from app.layers.spatial.border_region_trade import BorderRegionTrade
from app.layers.spatial.coastal_economic_concentration import CoastalEconomicConcentration
from app.layers.spatial.economic_geography_index import EconomicGeographyIndex
from app.layers.spatial.gentrification import Gentrification
from app.layers.spatial.geographic_isolation import GeographicIsolation
from app.layers.spatial.housing import HousingEconomics
from app.layers.spatial.infrastructure_density import InfrastructureDensity
from app.layers.spatial.land_area_productivity import LandAreaProductivity
from app.layers.spatial.land_value_capture import LandValueCapture
from app.layers.spatial.migration_economics import MigrationEconomics
from app.layers.spatial.natural_disaster_exposure import NaturalDisasterExposure
from app.layers.spatial.population_growth_spatial import PopulationGrowthSpatial
from app.layers.spatial.regional_convergence import RegionalConvergence
from app.layers.spatial.rural_urban_migration import RuralUrbanMigration
from app.layers.spatial.smart_cities import SmartCities
from app.layers.spatial.special_economic_zones import SEZEconomics
from app.layers.spatial.transportation import TransportEconomics
from app.layers.spatial.urban_primacy import UrbanPrimacy

ALL_MODULES = [
    Agglomeration,
    HousingEconomics,
    TransportEconomics,
    RegionalConvergence,
    MigrationEconomics,
    SEZEconomics,
    SmartCities,
    LandValueCapture,
    Gentrification,
    RuralUrbanMigration,
    UrbanPrimacy,
    GeographicIsolation,
    PopulationGrowthSpatial,
    LandAreaProductivity,
    CoastalEconomicConcentration,
    BorderRegionTrade,
    InfrastructureDensity,
    NaturalDisasterExposure,
    EconomicGeographyIndex,
]

__all__ = [
    "Agglomeration",
    "HousingEconomics",
    "TransportEconomics",
    "RegionalConvergence",
    "MigrationEconomics",
    "SEZEconomics",
    "SmartCities",
    "LandValueCapture",
    "Gentrification",
    "RuralUrbanMigration",
    "UrbanPrimacy",
    "GeographicIsolation",
    "PopulationGrowthSpatial",
    "LandAreaProductivity",
    "CoastalEconomicConcentration",
    "BorderRegionTrade",
    "InfrastructureDensity",
    "NaturalDisasterExposure",
    "EconomicGeographyIndex",
    "ALL_MODULES",
]
