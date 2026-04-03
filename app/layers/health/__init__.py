from app.layers.health.disease_burden import DiseaseBurden
from app.layers.health.health_expenditure import HealthExpenditure
from app.layers.health.health_insurance import HealthInsurance
from app.layers.health.nutrition import NutritionEconomics
from app.layers.health.pandemic_economics import PandemicEconomics
from app.layers.health.pharmaceutical import PharmaceuticalEconomics

ALL_MODULES = [
    HealthExpenditure,
    DiseaseBurden,
    HealthInsurance,
    PharmaceuticalEconomics,
    NutritionEconomics,
    PandemicEconomics,
]

__all__ = [
    "HealthExpenditure",
    "DiseaseBurden",
    "HealthInsurance",
    "PharmaceuticalEconomics",
    "NutritionEconomics",
    "PandemicEconomics",
    "ALL_MODULES",
]
