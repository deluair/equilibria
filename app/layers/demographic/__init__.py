from app.layers.demographic.aging import AgingEconomics
from app.layers.demographic.child_development import ChildDevelopment
from app.layers.demographic.fertility import FertilityEconomics
from app.layers.demographic.gender_economics import GenderEconomics
from app.layers.demographic.human_capital import HumanCapitalAccumulation
from app.layers.demographic.population_growth import PopulationGrowth

ALL_MODULES = [
    FertilityEconomics,
    AgingEconomics,
    HumanCapitalAccumulation,
    PopulationGrowth,
    GenderEconomics,
    ChildDevelopment,
]

__all__ = [
    "FertilityEconomics",
    "AgingEconomics",
    "HumanCapitalAccumulation",
    "PopulationGrowth",
    "GenderEconomics",
    "ChildDevelopment",
    "ALL_MODULES",
]
