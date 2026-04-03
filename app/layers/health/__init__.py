from app.layers.health.antimicrobial_resistance import AntimicrobialResistance
from app.layers.health.disease_burden import DiseaseBurden
from app.layers.health.health_expenditure import HealthExpenditure
from app.layers.health.health_insurance import HealthInsurance
from app.layers.health.health_workforce import HealthWorkforce
from app.layers.health.mental_health_economics import MentalHealthEconomics
from app.layers.health.nutrition import NutritionEconomics
from app.layers.health.pandemic_economics import PandemicEconomics
from app.layers.health.pharmaceutical import PharmaceuticalEconomics
from app.layers.health.telemedicine import Telemedicine

ALL_MODULES = [
    HealthExpenditure,
    DiseaseBurden,
    HealthInsurance,
    PharmaceuticalEconomics,
    NutritionEconomics,
    PandemicEconomics,
    MentalHealthEconomics,
    HealthWorkforce,
    AntimicrobialResistance,
    Telemedicine,
]

__all__ = [
    "HealthExpenditure",
    "DiseaseBurden",
    "HealthInsurance",
    "PharmaceuticalEconomics",
    "NutritionEconomics",
    "PandemicEconomics",
    "MentalHealthEconomics",
    "HealthWorkforce",
    "AntimicrobialResistance",
    "Telemedicine",
    "ALL_MODULES",
]
