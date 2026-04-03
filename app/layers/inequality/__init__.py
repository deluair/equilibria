from app.layers.inequality.consumption_inequality import ConsumptionInequality
from app.layers.inequality.gender_pay_gap import GenderPayGap
from app.layers.inequality.gini_dynamics import GiniDynamics
from app.layers.inequality.intergenerational_inequality import IntergenerationalInequality
from app.layers.inequality.labor_capital_split import LaborCapitalSplit
from app.layers.inequality.palma_ratio import PalmaRatio
from app.layers.inequality.poverty_inequality_trap import PovertyInequalityTrap
from app.layers.inequality.regional_inequality import RegionalInequality
from app.layers.inequality.social_mobility_index import SocialMobilityIndex
from app.layers.inequality.tax_progressivity import TaxProgressivity
from app.layers.inequality.top_income_share import TopIncomeShare
from app.layers.inequality.wealth_inequality import WealthInequality

ALL_MODULES = [
    GiniDynamics,
    TopIncomeShare,
    PalmaRatio,
    WealthInequality,
    RegionalInequality,
    GenderPayGap,
    IntergenerationalInequality,
    ConsumptionInequality,
    TaxProgressivity,
    LaborCapitalSplit,
    PovertyInequalityTrap,
    SocialMobilityIndex,
]

__all__ = [
    "GiniDynamics",
    "TopIncomeShare",
    "PalmaRatio",
    "WealthInequality",
    "RegionalInequality",
    "GenderPayGap",
    "IntergenerationalInequality",
    "ConsumptionInequality",
    "TaxProgressivity",
    "LaborCapitalSplit",
    "PovertyInequalityTrap",
    "SocialMobilityIndex",
    "ALL_MODULES",
]
