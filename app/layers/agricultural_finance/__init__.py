from app.layers.agricultural_finance.agricultural_credit_share import AgriculturalCreditShare
from app.layers.agricultural_finance.agricultural_interest_burden import AgriculturalInterestBurden
from app.layers.agricultural_finance.climate_smart_finance import ClimateSmartFinance
from app.layers.agricultural_finance.crop_insurance_coverage import CropInsuranceCoverage
from app.layers.agricultural_finance.input_finance_access import InputFinanceAccess
from app.layers.agricultural_finance.land_as_collateral import LandAsCollateral
from app.layers.agricultural_finance.microfinance_penetration import MicrofinancePenetration
from app.layers.agricultural_finance.rural_credit_access import RuralCreditAccess
from app.layers.agricultural_finance.smallholder_finance_gap import SmallholderFinanceGap
from app.layers.agricultural_finance.value_chain_finance import ValueChainFinance

ALL_MODULES = [
    RuralCreditAccess,
    AgriculturalCreditShare,
    MicrofinancePenetration,
    CropInsuranceCoverage,
    InputFinanceAccess,
    ValueChainFinance,
    LandAsCollateral,
    AgriculturalInterestBurden,
    SmallholderFinanceGap,
    ClimateSmartFinance,
]

__all__ = [
    "RuralCreditAccess",
    "AgriculturalCreditShare",
    "MicrofinancePenetration",
    "CropInsuranceCoverage",
    "InputFinanceAccess",
    "ValueChainFinance",
    "LandAsCollateral",
    "AgriculturalInterestBurden",
    "SmallholderFinanceGap",
    "ClimateSmartFinance",
    "ALL_MODULES",
]
