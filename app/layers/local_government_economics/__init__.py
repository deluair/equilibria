"""Local Government Economics layer (lLG) modules."""

from app.layers.local_government_economics.fiscal_decentralization_index import FiscalDecentralizationIndex
from app.layers.local_government_economics.local_revenue_adequacy import LocalRevenueAdequacy
from app.layers.local_government_economics.municipal_debt_stress import MunicipalDebtStress
from app.layers.local_government_economics.service_delivery_quality import ServiceDeliveryQuality
from app.layers.local_government_economics.local_government_capacity import LocalGovernmentCapacity
from app.layers.local_government_economics.intergovernmental_transfer_dependency import IntergovernmentalTransferDependency
from app.layers.local_government_economics.urban_rural_fiscal_gap import UrbanRuralFiscalGap
from app.layers.local_government_economics.participatory_budgeting_index import ParticipatoryBudgetingIndex
from app.layers.local_government_economics.local_economic_development import LocalEconomicDevelopment
from app.layers.local_government_economics.subnational_accountability import SubnationalAccountability

ALL_MODULES = [
    FiscalDecentralizationIndex,
    LocalRevenueAdequacy,
    MunicipalDebtStress,
    ServiceDeliveryQuality,
    LocalGovernmentCapacity,
    IntergovernmentalTransferDependency,
    UrbanRuralFiscalGap,
    ParticipatoryBudgetingIndex,
    LocalEconomicDevelopment,
    SubnationalAccountability,
]

__all__ = ["ALL_MODULES"]
