from app.layers.green_transition.decarbonization_rate import DecarbonizationRate
from app.layers.green_transition.renewable_energy_speed import RenewableEnergySpeed
from app.layers.green_transition.fossil_fuel_exit_pace import FossilFuelExitPace
from app.layers.green_transition.green_jobs_creation import GreenJobsCreation
from app.layers.green_transition.stranded_asset_risk import StrandedAssetRisk
from app.layers.green_transition.carbon_lock_in_index import CarbonLockInIndex
from app.layers.green_transition.energy_transition_investment import EnergyTransitionInvestment
from app.layers.green_transition.policy_ambition_gap import PolicyAmbitionGap
from app.layers.green_transition.just_transition_risk import JustTransitionRisk
from app.layers.green_transition.green_growth_decoupling import GreenGrowthDecoupling

ALL_MODULES = [
    DecarbonizationRate,
    RenewableEnergySpeed,
    FossilFuelExitPace,
    GreenJobsCreation,
    StrandedAssetRisk,
    CarbonLockInIndex,
    EnergyTransitionInvestment,
    PolicyAmbitionGap,
    JustTransitionRisk,
    GreenGrowthDecoupling,
]

__all__ = [
    "DecarbonizationRate",
    "RenewableEnergySpeed",
    "FossilFuelExitPace",
    "GreenJobsCreation",
    "StrandedAssetRisk",
    "CarbonLockInIndex",
    "EnergyTransitionInvestment",
    "PolicyAmbitionGap",
    "JustTransitionRisk",
    "GreenGrowthDecoupling",
    "ALL_MODULES",
]
