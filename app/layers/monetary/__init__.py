from app.layers.monetary.central_bank import CentralBankAnalysis
from app.layers.monetary.crypto_economics import CryptoEconomics
from app.layers.monetary.exchange_rate_models import ExchangeRateModels
from app.layers.monetary.financial_inclusion import FinancialInclusion
from app.layers.monetary.inflation_targeting import InflationTargeting
from app.layers.monetary.money_demand import MoneyDemand

ALL_MODULES = [
    MoneyDemand,
    CentralBankAnalysis,
    InflationTargeting,
    ExchangeRateModels,
    CryptoEconomics,
    FinancialInclusion,
]

__all__ = [
    "MoneyDemand",
    "CentralBankAnalysis",
    "InflationTargeting",
    "ExchangeRateModels",
    "CryptoEconomics",
    "FinancialInclusion",
    "ALL_MODULES",
]
