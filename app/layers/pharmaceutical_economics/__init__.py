from app.layers.pharmaceutical_economics.essential_medicines_access import EssentialMedicinesAccess
from app.layers.pharmaceutical_economics.pharmaceutical_spending_share import PharmaceuticalSpendingShare
from app.layers.pharmaceutical_economics.drug_affordability_index import DrugAffordabilityIndex
from app.layers.pharmaceutical_economics.generic_medicine_penetration import GenericMedicinePenetration
from app.layers.pharmaceutical_economics.health_rd_investment import HealthRdInvestment
from app.layers.pharmaceutical_economics.medicine_supply_chain import MedicineSupplyChain
from app.layers.pharmaceutical_economics.pharmaceutical_trade_balance import PharmaceuticalTradeBalance
from app.layers.pharmaceutical_economics.counterfeit_medicine_risk import CounterfeitMedicineRisk
from app.layers.pharmaceutical_economics.pandemic_medicine_preparedness import PandemicMedicinePreparedness
from app.layers.pharmaceutical_economics.medicine_regulatory_quality import MedicineRegulatoryQuality

ALL_MODULES = [
    EssentialMedicinesAccess,
    PharmaceuticalSpendingShare,
    DrugAffordabilityIndex,
    GenericMedicinePenetration,
    HealthRdInvestment,
    MedicineSupplyChain,
    PharmaceuticalTradeBalance,
    CounterfeitMedicineRisk,
    PandemicMedicinePreparedness,
    MedicineRegulatoryQuality,
]

__all__ = [
    "EssentialMedicinesAccess",
    "PharmaceuticalSpendingShare",
    "DrugAffordabilityIndex",
    "GenericMedicinePenetration",
    "HealthRdInvestment",
    "MedicineSupplyChain",
    "PharmaceuticalTradeBalance",
    "CounterfeitMedicineRisk",
    "PandemicMedicinePreparedness",
    "MedicineRegulatoryQuality",
    "ALL_MODULES",
]
