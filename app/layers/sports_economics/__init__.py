from app.layers.sports_economics.sports_gdp_contribution import SportsGDPContribution
from app.layers.sports_economics.mega_event_economic_impact import MegaEventEconomicImpact
from app.layers.sports_economics.athlete_labor_market import AthleteLaborMarket
from app.layers.sports_economics.stadium_infrastructure_economics import StadiumInfrastructureEconomics
from app.layers.sports_economics.sports_media_rights_value import SportMediaRightsValue
from app.layers.sports_economics.sports_betting_market_size import SportsBettingMarketSize
from app.layers.sports_economics.youth_sports_development import YouthSportsDevelopment
from app.layers.sports_economics.sports_tourism_contribution import SportsTourismContribution
from app.layers.sports_economics.esports_digital_economy import EsportsDigitalEconomy
from app.layers.sports_economics.sports_public_health_dividend import SportsPublicHealthDividend

ALL_MODULES = [
    SportsGDPContribution,
    MegaEventEconomicImpact,
    AthleteLaborMarket,
    StadiumInfrastructureEconomics,
    SportMediaRightsValue,
    SportsBettingMarketSize,
    YouthSportsDevelopment,
    SportsTourismContribution,
    EsportsDigitalEconomy,
    SportsPublicHealthDividend,
]

__all__ = [
    "SportsGDPContribution",
    "MegaEventEconomicImpact",
    "AthleteLaborMarket",
    "StadiumInfrastructureEconomics",
    "SportMediaRightsValue",
    "SportsBettingMarketSize",
    "YouthSportsDevelopment",
    "SportsTourismContribution",
    "EsportsDigitalEconomy",
    "SportsPublicHealthDividend",
    "ALL_MODULES",
]
