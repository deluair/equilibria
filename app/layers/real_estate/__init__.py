from app.layers.real_estate.commercial_real_estate import CommercialRealEstate
from app.layers.real_estate.construction_economics import ConstructionEconomics
from app.layers.real_estate.housing_affordability import HousingAffordability
from app.layers.real_estate.housing_price_index import HousingPriceIndex
from app.layers.real_estate.housing_supply_constraints import HousingSupplyConstraints
from app.layers.real_estate.land_value_taxation import LandValueTaxation
from app.layers.real_estate.mortgage_market import MortgageMarket
from app.layers.real_estate.real_estate_bubble import RealEstateBubble
from app.layers.real_estate.rental_market import RentalMarket
from app.layers.real_estate.urban_land_use import UrbanLandUse

ALL_MODULES = [
    HousingAffordability,
    RealEstateBubble,
    MortgageMarket,
    HousingSupplyConstraints,
    ConstructionEconomics,
    LandValueTaxation,
    UrbanLandUse,
    CommercialRealEstate,
    HousingPriceIndex,
    RentalMarket,
]

__all__ = [
    "HousingAffordability",
    "RealEstateBubble",
    "MortgageMarket",
    "HousingSupplyConstraints",
    "ConstructionEconomics",
    "LandValueTaxation",
    "UrbanLandUse",
    "CommercialRealEstate",
    "HousingPriceIndex",
    "RentalMarket",
    "ALL_MODULES",
]
