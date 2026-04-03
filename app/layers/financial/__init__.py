from app.layers.financial.banking_stability import BankingStability
from app.layers.financial.capm import CAPM
from app.layers.financial.contagion import FinancialContagion
from app.layers.financial.credit_risk import CreditRisk
from app.layers.financial.efficient_frontier import EfficientFrontier
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
    "ALL_MODULES",
]
