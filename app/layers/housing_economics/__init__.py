from app.layers.housing_economics.house_price_income_ratio import HousePriceIncomeRatio
from app.layers.housing_economics.housing_affordability_index import HousingAffordabilityIndex
from app.layers.housing_economics.residential_construction_gap import ResidentialConstructionGap
from app.layers.housing_economics.rent_burden_index import RentBurdenIndex
from app.layers.housing_economics.housing_bubble_indicator import HousingBubbleIndicator
from app.layers.housing_economics.mortgage_market_depth import MortgageMarketDepth
from app.layers.housing_economics.social_housing_gap import SocialHousingGap
from app.layers.housing_economics.housing_wealth_inequality import HousingWealthInequality
from app.layers.housing_economics.eviction_housing_stress import EvictionHousingStress
from app.layers.housing_economics.construction_cost_index import ConstructionCostIndex

ALL_MODULES = [
    HousePriceIncomeRatio,
    HousingAffordabilityIndex,
    ResidentialConstructionGap,
    RentBurdenIndex,
    HousingBubbleIndicator,
    MortgageMarketDepth,
    SocialHousingGap,
    HousingWealthInequality,
    EvictionHousingStress,
    ConstructionCostIndex,
]

__all__ = [
    "HousePriceIncomeRatio",
    "HousingAffordabilityIndex",
    "ResidentialConstructionGap",
    "RentBurdenIndex",
    "HousingBubbleIndicator",
    "MortgageMarketDepth",
    "SocialHousingGap",
    "HousingWealthInequality",
    "EvictionHousingStress",
    "ConstructionCostIndex",
    "ALL_MODULES",
]
