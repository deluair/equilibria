from app.layers.history.colonial_legacy import ColonialLegacy
from app.layers.history.demographic_transition import DemographicTransition
from app.layers.history.economic_revolutions import EconomicRevolutions
from app.layers.history.great_depression_analogy import GreatDepressionAnalogy
from app.layers.history.historical_inequality import HistoricalInequality
from app.layers.history.historical_trade_patterns import HistoricalTradePatterns
from app.layers.history.institutional_persistence import InstitutionalPersistence
from app.layers.history.long_run_growth import LongRunGrowth
from app.layers.history.technological_diffusion import TechnologicalDiffusion
from app.layers.history.war_economic_cost import WarEconomicCost

ALL_MODULES = [
    LongRunGrowth,
    ColonialLegacy,
    InstitutionalPersistence,
    HistoricalInequality,
    WarEconomicCost,
    TechnologicalDiffusion,
    GreatDepressionAnalogy,
    HistoricalTradePatterns,
    DemographicTransition,
    EconomicRevolutions,
]

__all__ = [
    "LongRunGrowth",
    "ColonialLegacy",
    "InstitutionalPersistence",
    "HistoricalInequality",
    "WarEconomicCost",
    "TechnologicalDiffusion",
    "GreatDepressionAnalogy",
    "HistoricalTradePatterns",
    "DemographicTransition",
    "EconomicRevolutions",
    "ALL_MODULES",
]
