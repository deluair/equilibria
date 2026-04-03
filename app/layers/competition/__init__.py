from app.layers.competition.antitrust_environment import AntitrustEnvironment
from app.layers.competition.competitive_dynamics import CompetitiveDynamics
from app.layers.competition.entry_barriers import EntryBarriers
from app.layers.competition.market_concentration import MarketConcentration
from app.layers.competition.market_contestability import MarketContestability
from app.layers.competition.markup_estimation import MarkupEstimation
from app.layers.competition.monopoly_rent import MonopolyRent
from app.layers.competition.network_monopoly import NetworkMonopoly
from app.layers.competition.state_owned_enterprise import StateOwnedEnterprise
from app.layers.competition.trade_competition import TradeCompetition

ALL_MODULES = [
    MarketConcentration,
    MarkupEstimation,
    EntryBarriers,
    MarketContestability,
    StateOwnedEnterprise,
    MonopolyRent,
    CompetitiveDynamics,
    AntitrustEnvironment,
    TradeCompetition,
    NetworkMonopoly,
]

__all__ = [
    "MarketConcentration",
    "MarkupEstimation",
    "EntryBarriers",
    "MarketContestability",
    "StateOwnedEnterprise",
    "MonopolyRent",
    "CompetitiveDynamics",
    "AntitrustEnvironment",
    "TradeCompetition",
    "NetworkMonopoly",
    "ALL_MODULES",
]
