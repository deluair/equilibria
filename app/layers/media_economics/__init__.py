from app.layers.media_economics.media_market_concentration import MediaMarketConcentration
from app.layers.media_economics.news_desert_spread import NewsDesertSpread
from app.layers.media_economics.digital_ad_domination import DigitalAdDomination
from app.layers.media_economics.journalism_economic_viability import JournalismEconomicViability
from app.layers.media_economics.platform_news_subsidy import PlatformNewsSubsidy
from app.layers.media_economics.media_pluralism_index import MediaPluralismIndex
from app.layers.media_economics.subscription_economics_press import SubscriptionEconomicsPress
from app.layers.media_economics.information_ecosystem_health import InformationEcosystemHealth
from app.layers.media_economics.digital_misinformation_cost import DigitalMisinformationCost
from app.layers.media_economics.media_ownership_transparency import MediaOwnershipTransparency

ALL_MODULES = [
    MediaMarketConcentration,
    NewsDesertSpread,
    DigitalAdDomination,
    JournalismEconomicViability,
    PlatformNewsSubsidy,
    MediaPluralismIndex,
    SubscriptionEconomicsPress,
    InformationEcosystemHealth,
    DigitalMisinformationCost,
    MediaOwnershipTransparency,
]

__all__ = [
    "MediaMarketConcentration",
    "NewsDesertSpread",
    "DigitalAdDomination",
    "JournalismEconomicViability",
    "PlatformNewsSubsidy",
    "MediaPluralismIndex",
    "SubscriptionEconomicsPress",
    "InformationEcosystemHealth",
    "DigitalMisinformationCost",
    "MediaOwnershipTransparency",
    "ALL_MODULES",
]
