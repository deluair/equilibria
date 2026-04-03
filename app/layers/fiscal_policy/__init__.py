from app.layers.fiscal_policy.fiscal_deficit_trend import FiscalDeficitTrend
from app.layers.fiscal_policy.primary_balance import PrimaryBalance
from app.layers.fiscal_policy.expenditure_composition import ExpenditureComposition
from app.layers.fiscal_policy.tax_buoyancy import TaxBuoyancy
from app.layers.fiscal_policy.debt_to_revenue_ratio import DebtToRevenueRatio
from app.layers.fiscal_policy.fiscal_space_index import FiscalSpaceIndex
from app.layers.fiscal_policy.countercyclical_fiscal import CountercyclicalFiscal
from app.layers.fiscal_policy.revenue_diversification import RevenueDiversification
from app.layers.fiscal_policy.fiscal_multiplier_conditions import FiscalMultiplierConditions
from app.layers.fiscal_policy.arrears_fiscal_risk import ArrearsFiscalRisk

ALL_MODULES = [
    FiscalDeficitTrend,
    PrimaryBalance,
    ExpenditureComposition,
    TaxBuoyancy,
    DebtToRevenueRatio,
    FiscalSpaceIndex,
    CountercyclicalFiscal,
    RevenueDiversification,
    FiscalMultiplierConditions,
    ArrearsFiscalRisk,
]

__all__ = [
    "FiscalDeficitTrend",
    "PrimaryBalance",
    "ExpenditureComposition",
    "TaxBuoyancy",
    "DebtToRevenueRatio",
    "FiscalSpaceIndex",
    "CountercyclicalFiscal",
    "RevenueDiversification",
    "FiscalMultiplierConditions",
    "ArrearsFiscalRisk",
    "ALL_MODULES",
]
