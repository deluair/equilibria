from app.layers.monetary.capital_controls import CapitalControls
from app.layers.monetary.central_bank import CentralBankAnalysis
from app.layers.monetary.central_bank_independence import CentralBankIndependence
from app.layers.monetary.credit_to_gdp_gap import CreditToGDPGap
from app.layers.monetary.crypto_economics import CryptoEconomics
from app.layers.monetary.currency_substitution import CurrencySubstitution
from app.layers.monetary.digital_currency import DigitalCurrency
from app.layers.monetary.dollarization import Dollarization
from app.layers.monetary.exchange_rate_models import ExchangeRateModels
from app.layers.monetary.financial_inclusion import FinancialInclusion
from app.layers.monetary.financial_repression import FinancialRepression
from app.layers.monetary.inflation_targeting import InflationTargeting
from app.layers.monetary.monetary_base_growth import MonetaryBaseGrowth
from app.layers.monetary.monetary_overhang import MonetaryOverhang
from app.layers.monetary.money_demand import MoneyDemand
from app.layers.monetary.money_multiplier import MoneyMultiplier
from app.layers.monetary.optimal_currency_area import OptimalCurrencyArea
from app.layers.monetary.real_money_demand import RealMoneyDemand
from app.layers.monetary.reserve_currency import ReserveCurrency
from app.layers.monetary.seigniorage_revenue import SeigniorageRevenue

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
    MoneyMultiplier,
    CreditToGDPGap,
    MonetaryBaseGrowth,
    CurrencySubstitution,
    RealMoneyDemand,
    CentralBankIndependence,
    SeigniorageRevenue,
    MonetaryOverhang,
    FinancialRepression,
    OptimalCurrencyArea,
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
    "MoneyMultiplier",
    "CreditToGDPGap",
    "MonetaryBaseGrowth",
    "CurrencySubstitution",
    "RealMoneyDemand",
    "CentralBankIndependence",
    "SeigniorageRevenue",
    "MonetaryOverhang",
    "FinancialRepression",
    "OptimalCurrencyArea",
    "ALL_MODULES",
]
