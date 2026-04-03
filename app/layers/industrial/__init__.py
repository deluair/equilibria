from app.layers.industrial.antitrust import AntitrustAnalysis
from app.layers.industrial.innovation import InnovationEconomics
from app.layers.industrial.market_structure import MarketStructure
from app.layers.industrial.merger_analysis import MergerAnalysis
from app.layers.industrial.platform_economics import PlatformEconomics
from app.layers.industrial.price_discrimination import PriceDiscrimination

ALL_MODULES = [
    MarketStructure,
    MergerAnalysis,
    PriceDiscrimination,
    InnovationEconomics,
    PlatformEconomics,
    AntitrustAnalysis,
]

__all__ = [
    "MarketStructure",
    "MergerAnalysis",
    "PriceDiscrimination",
    "InnovationEconomics",
    "PlatformEconomics",
    "AntitrustAnalysis",
    "ALL_MODULES",
]
