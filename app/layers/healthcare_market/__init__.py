from app.layers.healthcare_market.healthcare_market_concentration import HealthcareMarketConcentration
from app.layers.healthcare_market.health_insurance_coverage import HealthInsuranceCoverage
from app.layers.healthcare_market.provider_competition_index import ProviderCompetitionIndex
from app.layers.healthcare_market.healthcare_price_regulation import HealthcarePriceRegulation
from app.layers.healthcare_market.market_failure_healthcare import MarketFailureHealthcare
from app.layers.healthcare_market.private_sector_health_share import PrivateSectorHealthShare
from app.layers.healthcare_market.healthcare_access_inequality import HealthcareAccessInequality
from app.layers.healthcare_market.preventive_vs_curative_balance import PreventiveVsCurativeBalance
from app.layers.healthcare_market.health_workforce_market import HealthWorkforceMarket
from app.layers.healthcare_market.pharmaceutical_market_access import PharmaceuticalMarketAccess

ALL_MODULES = [
    HealthcareMarketConcentration,
    HealthInsuranceCoverage,
    ProviderCompetitionIndex,
    HealthcarePriceRegulation,
    MarketFailureHealthcare,
    PrivateSectorHealthShare,
    HealthcareAccessInequality,
    PreventiveVsCurativeBalance,
    HealthWorkforceMarket,
    PharmaceuticalMarketAccess,
]

__all__ = [
    "HealthcareMarketConcentration",
    "HealthInsuranceCoverage",
    "ProviderCompetitionIndex",
    "HealthcarePriceRegulation",
    "MarketFailureHealthcare",
    "PrivateSectorHealthShare",
    "HealthcareAccessInequality",
    "PreventiveVsCurativeBalance",
    "HealthWorkforceMarket",
    "PharmaceuticalMarketAccess",
    "ALL_MODULES",
]
