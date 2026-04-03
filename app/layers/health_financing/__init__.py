from app.layers.health_financing.catastrophic_health_spending import CatastrophicHealthSpending
from app.layers.health_financing.disease_financing_gap import DiseaseFinancingGap
from app.layers.health_financing.domestic_health_financing import DomesticHealthFinancing
from app.layers.health_financing.health_expenditure_share import HealthExpenditureShare
from app.layers.health_financing.health_fiscal_space import HealthFiscalSpace
from app.layers.health_financing.health_system_efficiency import HealthSystemEfficiency
from app.layers.health_financing.health_workforce_economics import HealthWorkforceEconomics
from app.layers.health_financing.oop_health_burden import OopHealthBurden
from app.layers.health_financing.pharmaceutical_affordability import PharmaceuticalAffordability
from app.layers.health_financing.prepayment_coverage import PrepaymentCoverage

ALL_MODULES = [
    HealthExpenditureShare,
    OopHealthBurden,
    CatastrophicHealthSpending,
    PrepaymentCoverage,
    HealthSystemEfficiency,
    DomesticHealthFinancing,
    HealthFiscalSpace,
    PharmaceuticalAffordability,
    HealthWorkforceEconomics,
    DiseaseFinancingGap,
]

__all__ = [
    "HealthExpenditureShare",
    "OopHealthBurden",
    "CatastrophicHealthSpending",
    "PrepaymentCoverage",
    "HealthSystemEfficiency",
    "DomesticHealthFinancing",
    "HealthFiscalSpace",
    "PharmaceuticalAffordability",
    "HealthWorkforceEconomics",
    "DiseaseFinancingGap",
    "ALL_MODULES",
]
