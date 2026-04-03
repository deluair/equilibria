from app.layers.demographic.aging import AgingEconomics
from app.layers.demographic.child_development import ChildDevelopment
from app.layers.demographic.fertility import FertilityEconomics
from app.layers.demographic.gender_economics import GenderEconomics
from app.layers.demographic.human_capital import HumanCapitalAccumulation
from app.layers.demographic.life_expectancy import LifeExpectancy
from app.layers.demographic.population_density_stress import PopulationDensityStress
from app.layers.demographic.population_growth import PopulationGrowth
from app.layers.demographic.urbanization import Urbanization
from app.layers.demographic.youth_bulge import YouthBulge

ALL_MODULES = [
    FertilityEconomics,
    AgingEconomics,
    HumanCapitalAccumulation,
    PopulationGrowth,
    GenderEconomics,
    ChildDevelopment,
    Urbanization,
    LifeExpectancy,
    YouthBulge,
    PopulationDensityStress,
]

__all__ = [
    "FertilityEconomics",
    "AgingEconomics",
    "HumanCapitalAccumulation",
    "PopulationGrowth",
    "GenderEconomics",
    "ChildDevelopment",
    "Urbanization",
    "LifeExpectancy",
    "YouthBulge",
    "PopulationDensityStress",
    "ALL_MODULES",
]
