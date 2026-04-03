from app.layers.regional_development.agglomeration_economics import AgglomerationEconomics
from app.layers.regional_development.border_region_development import BorderRegionDevelopment
from app.layers.regional_development.geographic_disadvantage import GeographicDisadvantage
from app.layers.regional_development.lagging_regions import LaggingRegions
from app.layers.regional_development.regional_convergence import RegionalConvergence
from app.layers.regional_development.regional_fiscal_transfer import RegionalFiscalTransfer
from app.layers.regional_development.regional_inequality_index import RegionalInequalityIndex
from app.layers.regional_development.regional_infrastructure import RegionalInfrastructure
from app.layers.regional_development.resource_region_dependency import ResourceRegionDependency
from app.layers.regional_development.special_economic_zones import SpecialEconomicZones

ALL_MODULES = [
    RegionalConvergence,
    LaggingRegions,
    RegionalInfrastructure,
    ResourceRegionDependency,
    BorderRegionDevelopment,
    RegionalFiscalTransfer,
    AgglomerationEconomics,
    GeographicDisadvantage,
    RegionalInequalityIndex,
    SpecialEconomicZones,
]

__all__ = [
    "RegionalConvergence",
    "LaggingRegions",
    "RegionalInfrastructure",
    "ResourceRegionDependency",
    "BorderRegionDevelopment",
    "RegionalFiscalTransfer",
    "AgglomerationEconomics",
    "GeographicDisadvantage",
    "RegionalInequalityIndex",
    "SpecialEconomicZones",
    "ALL_MODULES",
]
