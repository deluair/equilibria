from app.layers.disaster_economics.climate_shock_exposure import ClimateShockExposure
from app.layers.disaster_economics.conflict_disaster_nexus import ConflictDisasterNexus
from app.layers.disaster_economics.debt_disaster_trap import DebtDisasterTrap
from app.layers.disaster_economics.disaster_recovery_capacity import DisasterRecoveryCapacity
from app.layers.disaster_economics.disaster_vulnerability import DisasterVulnerability
from app.layers.disaster_economics.economic_losses_from_disasters import EconomicLossesFromDisasters
from app.layers.disaster_economics.food_system_resilience import FoodSystemResilience
from app.layers.disaster_economics.insurance_coverage_gap import InsuranceCoverageGap
from app.layers.disaster_economics.pandemic_preparedness import PandemicPreparedness
from app.layers.disaster_economics.social_cohesion_resilience import SocialCohesionResilience

ALL_MODULES = [
    DisasterVulnerability,
    PandemicPreparedness,
    DisasterRecoveryCapacity,
    FoodSystemResilience,
    ClimateShockExposure,
    ConflictDisasterNexus,
    EconomicLossesFromDisasters,
    InsuranceCoverageGap,
    DebtDisasterTrap,
    SocialCohesionResilience,
]

__all__ = [
    "DisasterVulnerability",
    "PandemicPreparedness",
    "DisasterRecoveryCapacity",
    "FoodSystemResilience",
    "ClimateShockExposure",
    "ConflictDisasterNexus",
    "EconomicLossesFromDisasters",
    "InsuranceCoverageGap",
    "DebtDisasterTrap",
    "SocialCohesionResilience",
    "ALL_MODULES",
]
