from app.layers.climate_finance.adaptation_finance_gap import AdaptationFinanceGap
from app.layers.climate_finance.carbon_price_signal import CarbonPriceSignal
from app.layers.climate_finance.climate_fiscal_risk import ClimateFiscalRisk
from app.layers.climate_finance.climate_investment_gap import ClimateInvestmentGap
from app.layers.climate_finance.climate_risk_financial import ClimateRiskFinancial
from app.layers.climate_finance.fossil_fuel_subsidy_reform import FossilFuelSubsidyReform
from app.layers.climate_finance.green_bond_market import GreenBondMarket
from app.layers.climate_finance.green_taxonomy_alignment import GreenTaxonomyAlignment
from app.layers.climate_finance.just_transition_finance import JustTransitionFinance
from app.layers.climate_finance.loss_damage_financing import LossDamageFinancing

ALL_MODULES = [
    GreenBondMarket,
    ClimateInvestmentGap,
    CarbonPriceSignal,
    FossilFuelSubsidyReform,
    ClimateRiskFinancial,
    GreenTaxonomyAlignment,
    AdaptationFinanceGap,
    LossDamageFinancing,
    JustTransitionFinance,
    ClimateFiscalRisk,
]

__all__ = [
    "GreenBondMarket",
    "ClimateInvestmentGap",
    "CarbonPriceSignal",
    "FossilFuelSubsidyReform",
    "ClimateRiskFinancial",
    "GreenTaxonomyAlignment",
    "AdaptationFinanceGap",
    "LossDamageFinancing",
    "JustTransitionFinance",
    "ClimateFiscalRisk",
    "ALL_MODULES",
]
