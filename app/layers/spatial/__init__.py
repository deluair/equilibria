from app.layers.spatial.agglomeration import Agglomeration
from app.layers.spatial.gentrification import Gentrification
from app.layers.spatial.housing import HousingEconomics
from app.layers.spatial.land_value_capture import LandValueCapture
from app.layers.spatial.migration_economics import MigrationEconomics
from app.layers.spatial.regional_convergence import RegionalConvergence
from app.layers.spatial.rural_urban_migration import RuralUrbanMigration
from app.layers.spatial.smart_cities import SmartCities
from app.layers.spatial.special_economic_zones import SEZEconomics
from app.layers.spatial.transportation import TransportEconomics

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
    "ALL_MODULES",
]
