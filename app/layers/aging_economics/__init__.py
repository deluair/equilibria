from app.layers.aging_economics.silver_economy_size import SilverEconomySize
from app.layers.aging_economics.healthcare_aging_cost import HealthcareAgingCost
from app.layers.aging_economics.pension_fiscal_burden import PensionFiscalBurden
from app.layers.aging_economics.workforce_aging_productivity import WorkforceAgingProductivity
from app.layers.aging_economics.longevity_risk_index import LongevityRiskIndex
from app.layers.aging_economics.active_aging_index import ActiveAgingIndex
from app.layers.aging_economics.intergenerational_transfer import IntergenerationalTransfer
from app.layers.aging_economics.dementia_economic_burden import DementiaEconomicBurden
from app.layers.aging_economics.eldercare_infrastructure_gap import ElderCareInfrastructureGap
from app.layers.aging_economics.aging_innovation_paradox import AgingInnovationParadox

ALL_MODULES = [
    SilverEconomySize,
    HealthcareAgingCost,
    PensionFiscalBurden,
    WorkforceAgingProductivity,
    LongevityRiskIndex,
    ActiveAgingIndex,
    IntergenerationalTransfer,
    DementiaEconomicBurden,
    ElderCareInfrastructureGap,
    AgingInnovationParadox,
]

__all__ = [
    "SilverEconomySize",
    "HealthcareAgingCost",
    "PensionFiscalBurden",
    "WorkforceAgingProductivity",
    "LongevityRiskIndex",
    "ActiveAgingIndex",
    "IntergenerationalTransfer",
    "DementiaEconomicBurden",
    "ElderCareInfrastructureGap",
    "AgingInnovationParadox",
    "ALL_MODULES",
]
