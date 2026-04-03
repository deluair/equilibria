from app.layers.labor_institutions.union_density import UnionDensity
from app.layers.labor_institutions.employment_protection_index import EmploymentProtectionIndex
from app.layers.labor_institutions.minimum_wage_bite import MinimumWageBite
from app.layers.labor_institutions.collective_bargaining_coverage import CollectiveBargainingCoverage
from app.layers.labor_institutions.wage_coordination_index import WageCoordinationIndex
from app.layers.labor_institutions.labor_dispute_intensity import LaborDisputeIntensity
from app.layers.labor_institutions.informal_employment_share import InformalEmploymentShare
from app.layers.labor_institutions.active_labor_market_spending import ActiveLaborMarketSpending
from app.layers.labor_institutions.worker_voice_index import WorkerVoiceIndex
from app.layers.labor_institutions.labor_regulation_quality import LaborRegulationQuality

ALL_MODULES = [
    UnionDensity,
    EmploymentProtectionIndex,
    MinimumWageBite,
    CollectiveBargainingCoverage,
    WageCoordinationIndex,
    LaborDisputeIntensity,
    InformalEmploymentShare,
    ActiveLaborMarketSpending,
    WorkerVoiceIndex,
    LaborRegulationQuality,
]

__all__ = [
    "UnionDensity",
    "EmploymentProtectionIndex",
    "MinimumWageBite",
    "CollectiveBargainingCoverage",
    "WageCoordinationIndex",
    "LaborDisputeIntensity",
    "InformalEmploymentShare",
    "ActiveLaborMarketSpending",
    "WorkerVoiceIndex",
    "LaborRegulationQuality",
    "ALL_MODULES",
]
