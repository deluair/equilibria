from app.layers.financial.bank_runs import BankRuns
from app.layers.financial.banking_stability import BankingStability
from app.layers.financial.capm import CAPM
from app.layers.financial.contagion import FinancialContagion
from app.layers.financial.credit_risk import CreditRisk
from app.layers.financial.efficient_frontier import EfficientFrontier
from app.layers.financial.fintech_disruption import FintechDisruption
from app.layers.financial.insurance_economics import InsuranceEconomics
from app.layers.financial.sovereign_debt import SovereignDebt
from app.layers.financial.term_structure import TermStructure
from app.layers.financial.var_risk import ValueAtRisk
from app.layers.financial.volatility_modeling import VolatilityModeling

ALL_MODULES = [
    CAPM,
    EfficientFrontier,
    ValueAtRisk,
    CreditRisk,
    TermStructure,
    VolatilityModeling,
    FinancialContagion,
    BankingStability,
    FintechDisruption,
    InsuranceEconomics,
    SovereignDebt,
    BankRuns,
]

__all__ = [
    "CAPM",
    "EfficientFrontier",
    "ValueAtRisk",
    "CreditRisk",
    "TermStructure",
    "VolatilityModeling",
    "FinancialContagion",
    "BankingStability",
    "FintechDisruption",
    "InsuranceEconomics",
    "SovereignDebt",
    "BankRuns",
    "ALL_MODULES",
]
