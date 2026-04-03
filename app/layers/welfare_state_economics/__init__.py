"""Welfare State Economics layer (lWS) — 10 modules."""

from app.layers.welfare_state_economics.benefit_adequacy_index import BenefitAdequacyIndex
from app.layers.welfare_state_economics.decommodification_index import DecommodificationIndex
from app.layers.welfare_state_economics.social_expenditure_generosity import SocialExpenditureGenerosity
from app.layers.welfare_state_economics.universalbasic_income_readiness import UniversalBasicIncomeReadiness
from app.layers.welfare_state_economics.universalism_index import UniversalismIndex
from app.layers.welfare_state_economics.welfare_fiscal_sustainability import WelfareFiscalSustainability
from app.layers.welfare_state_economics.welfare_inequality_nexus import WelfareInequalityNexus
from app.layers.welfare_state_economics.welfare_labor_market_interaction import WelfareLaborMarketInteraction
from app.layers.welfare_state_economics.welfare_poverty_reduction import WelfarePovertyReduction
from app.layers.welfare_state_economics.welfare_state_maturity import WelfareStateMaturity

ALL_MODULES = [
    SocialExpenditureGenerosity,
    UniversalismIndex,
    WelfareFiscalSustainability,
    WelfarePovertyReduction,
    BenefitAdequacyIndex,
    WelfareLaborMarketInteraction,
    UniversalBasicIncomeReadiness,
    WelfareStateMaturity,
    DecommodificationIndex,
    WelfareInequalityNexus,
]

__all__ = [
    "ALL_MODULES",
    "SocialExpenditureGenerosity",
    "UniversalismIndex",
    "WelfareFiscalSustainability",
    "WelfarePovertyReduction",
    "BenefitAdequacyIndex",
    "WelfareLaborMarketInteraction",
    "UniversalBasicIncomeReadiness",
    "WelfareStateMaturity",
    "DecommodificationIndex",
    "WelfareInequalityNexus",
]
