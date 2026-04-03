from app.layers.industrial.antitrust import AntitrustAnalysis
from app.layers.industrial.creative_destruction import CreativeDestruction
from app.layers.industrial.digital_markets import DigitalMarkets
from app.layers.industrial.innovation import InnovationEconomics
from app.layers.industrial.market_structure import MarketStructure
from app.layers.industrial.merger_analysis import MergerAnalysis
from app.layers.industrial.network_industries import NetworkIndustries
from app.layers.industrial.platform_economics import PlatformEconomics
from app.layers.industrial.price_discrimination import PriceDiscrimination
from app.layers.industrial.startup_economics import StartupEconomics

ALL_MODULES = [
    MarketStructure,
    MergerAnalysis,
    PriceDiscrimination,
    InnovationEconomics,
    PlatformEconomics,
    AntitrustAnalysis,
    DigitalMarkets,
    NetworkIndustries,
    StartupEconomics,
    CreativeDestruction,
]

__all__ = [
    "MarketStructure",
    "MergerAnalysis",
    "PriceDiscrimination",
    "InnovationEconomics",
    "PlatformEconomics",
    "AntitrustAnalysis",
    "DigitalMarkets",
    "NetworkIndustries",
    "StartupEconomics",
    "CreativeDestruction",
    "ALL_MODULES",
]
