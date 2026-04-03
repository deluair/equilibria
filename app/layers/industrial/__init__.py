from app.layers.industrial.antitrust import AntitrustAnalysis
from app.layers.industrial.business_environment import BusinessEnvironment
from app.layers.industrial.creative_destruction import CreativeDestruction
from app.layers.industrial.digital_markets import DigitalMarkets
from app.layers.industrial.export_sophistication import ExportSophistication
from app.layers.industrial.fdi_quality import FDIQuality
from app.layers.industrial.industrial_energy_intensity import IndustrialEnergyIntensity
from app.layers.industrial.industry_diversification import IndustryDiversification
from app.layers.industrial.innovation import InnovationEconomics
from app.layers.industrial.labor_intensive_exports import LaborIntensiveExports
from app.layers.industrial.manufacturing_value_added import ManufacturingValueAdded
from app.layers.industrial.market_structure import MarketStructure
from app.layers.industrial.merger_analysis import MergerAnalysis
from app.layers.industrial.network_industries import NetworkIndustries
from app.layers.industrial.platform_economics import PlatformEconomics
from app.layers.industrial.price_discrimination import PriceDiscrimination
from app.layers.industrial.sme_development import SMEDevelopment
from app.layers.industrial.startup_activity import StartupActivity
from app.layers.industrial.startup_economics import StartupEconomics
from app.layers.industrial.supply_chain_resilience import SupplyChainResilience

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
    ManufacturingValueAdded,
    ExportSophistication,
    BusinessEnvironment,
    StartupActivity,
    IndustryDiversification,
    SupplyChainResilience,
    FDIQuality,
    IndustrialEnergyIntensity,
    LaborIntensiveExports,
    SMEDevelopment,
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
    "ManufacturingValueAdded",
    "ExportSophistication",
    "BusinessEnvironment",
    "StartupActivity",
    "IndustryDiversification",
    "SupplyChainResilience",
    "FDIQuality",
    "IndustrialEnergyIntensity",
    "LaborIntensiveExports",
    "SMEDevelopment",
    "ALL_MODULES",
]
