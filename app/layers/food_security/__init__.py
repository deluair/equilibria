from app.layers.food_security.food_availability import FoodAvailability
from app.layers.food_security.food_price_inflation import FoodPriceInflation
from app.layers.food_security.undernourishment_gap import UndernourishmentGap
from app.layers.food_security.stunting_wasting import StuntingWasting
from app.layers.food_security.food_import_vulnerability import FoodImportVulnerability
from app.layers.food_security.agricultural_productivity_gap import AgriculturalProductivityGap
from app.layers.food_security.water_food_nexus import WaterFoodNexus
from app.layers.food_security.food_system_shocks import FoodSystemShocks
from app.layers.food_security.nutrition_transition import NutritionTransition
from app.layers.food_security.food_governance import FoodGovernance

ALL_MODULES = [
    FoodAvailability,
    FoodPriceInflation,
    UndernourishmentGap,
    StuntingWasting,
    FoodImportVulnerability,
    AgriculturalProductivityGap,
    WaterFoodNexus,
    FoodSystemShocks,
    NutritionTransition,
    FoodGovernance,
]

__all__ = [
    "FoodAvailability",
    "FoodPriceInflation",
    "UndernourishmentGap",
    "StuntingWasting",
    "FoodImportVulnerability",
    "AgriculturalProductivityGap",
    "WaterFoodNexus",
    "FoodSystemShocks",
    "NutritionTransition",
    "FoodGovernance",
    "ALL_MODULES",
]
