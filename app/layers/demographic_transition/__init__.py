from app.layers.demographic_transition.fertility_rate_transition import FertilityRateTransition
from app.layers.demographic_transition.demographic_dividend_window import DemographicDividendWindow
from app.layers.demographic_transition.old_age_dependency_stress import OldAgeDependencyStress
from app.layers.demographic_transition.youth_bulge_risk import YouthBulgeRisk
from app.layers.demographic_transition.life_expectancy_gains import LifeExpectancyGains
from app.layers.demographic_transition.population_growth_stress import PopulationGrowthStress
from app.layers.demographic_transition.urbanization_transition import UrbanizationTransition
from app.layers.demographic_transition.child_mortality_decline import ChildMortalityDecline
from app.layers.demographic_transition.aging_workforce_pressure import AgingWorkforcePressure
from app.layers.demographic_transition.migration_demographic_balance import MigrationDemographicBalance

ALL_MODULES = [
    FertilityRateTransition,
    DemographicDividendWindow,
    OldAgeDependencyStress,
    YouthBulgeRisk,
    LifeExpectancyGains,
    PopulationGrowthStress,
    UrbanizationTransition,
    ChildMortalityDecline,
    AgingWorkforcePressure,
    MigrationDemographicBalance,
]

__all__ = [
    "FertilityRateTransition",
    "DemographicDividendWindow",
    "OldAgeDependencyStress",
    "YouthBulgeRisk",
    "LifeExpectancyGains",
    "PopulationGrowthStress",
    "UrbanizationTransition",
    "ChildMortalityDecline",
    "AgingWorkforcePressure",
    "MigrationDemographicBalance",
    "ALL_MODULES",
]
