from app.layers.urban_planning_economics.affordable_housing_policy import AffordableHousingPolicy
from app.layers.urban_planning_economics.building_code_compliance import BuildingCodeCompliance
from app.layers.urban_planning_economics.land_use_efficiency import LandUseEfficiency
from app.layers.urban_planning_economics.mixed_use_development import MixedUseDevelopment
from app.layers.urban_planning_economics.public_space_access import PublicSpaceAccess
from app.layers.urban_planning_economics.transit_oriented_development import TransitOrientedDevelopment
from app.layers.urban_planning_economics.urban_fiscal_autonomy import UrbanFiscalAutonomy
from app.layers.urban_planning_economics.urban_green_space_gap import UrbanGreenSpaceGap
from app.layers.urban_planning_economics.urban_sprawl_index import UrbanSprawlIndex
from app.layers.urban_planning_economics.zoning_flexibility_index import ZoningFlexibilityIndex

ALL_MODULES = [
    LandUseEfficiency,
    MixedUseDevelopment,
    TransitOrientedDevelopment,
    PublicSpaceAccess,
    ZoningFlexibilityIndex,
    UrbanSprawlIndex,
    AffordableHousingPolicy,
    UrbanGreenSpaceGap,
    BuildingCodeCompliance,
    UrbanFiscalAutonomy,
]
