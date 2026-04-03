from app.layers.digital_economy.ecommerce_penetration import EcommercePenetration
from app.layers.digital_economy.platform_economy_share import PlatformEconomyShare
from app.layers.digital_economy.digital_gdp_contribution import DigitalGdpContribution
from app.layers.digital_economy.internet_economy_size import InternetEconomySize
from app.layers.digital_economy.automation_displacement_risk import AutomationDisplacementRisk
from app.layers.digital_economy.data_economy_value import DataEconomyValue
from app.layers.digital_economy.digital_trade_intensity import DigitalTradeIntensity
from app.layers.digital_economy.platform_concentration_risk import PlatformConcentrationRisk
from app.layers.digital_economy.digital_productivity_premium import DigitalProductivityPremium
from app.layers.digital_economy.cyber_economic_risk import CyberEconomicRisk

ALL_MODULES = [
    EcommercePenetration,
    PlatformEconomyShare,
    DigitalGdpContribution,
    InternetEconomySize,
    AutomationDisplacementRisk,
    DataEconomyValue,
    DigitalTradeIntensity,
    PlatformConcentrationRisk,
    DigitalProductivityPremium,
    CyberEconomicRisk,
]

__all__ = [
    "EcommercePenetration",
    "PlatformEconomyShare",
    "DigitalGdpContribution",
    "InternetEconomySize",
    "AutomationDisplacementRisk",
    "DataEconomyValue",
    "DigitalTradeIntensity",
    "PlatformConcentrationRisk",
    "DigitalProductivityPremium",
    "CyberEconomicRisk",
    "ALL_MODULES",
]
