from app.layers.monetary.capital_controls import CapitalControls
from app.layers.monetary.central_bank import CentralBankAnalysis
from app.layers.monetary.crypto_economics import CryptoEconomics
from app.layers.monetary.digital_currency import DigitalCurrency
from app.layers.monetary.dollarization import Dollarization
from app.layers.monetary.exchange_rate_models import ExchangeRateModels
from app.layers.monetary.financial_inclusion import FinancialInclusion
from app.layers.monetary.inflation_targeting import InflationTargeting
from app.layers.monetary.money_demand import MoneyDemand
from app.layers.monetary.reserve_currency import ReserveCurrency

ALL_MODULES = [
    MoneyDemand,
    CentralBankAnalysis,
    InflationTargeting,
    ExchangeRateModels,
    CryptoEconomics,
    FinancialInclusion,
    DigitalCurrency,
    Dollarization,
    CapitalControls,
    ReserveCurrency,
]

__all__ = [
    "MoneyDemand",
    "CentralBankAnalysis",
    "InflationTargeting",
    "ExchangeRateModels",
    "CryptoEconomics",
    "FinancialInclusion",
    "DigitalCurrency",
    "Dollarization",
    "CapitalControls",
    "ReserveCurrency",
    "ALL_MODULES",
]
