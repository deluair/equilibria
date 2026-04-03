from app.layers.political.anticorruption_effectiveness import AnticorruptionEffectiveness
from app.layers.political.civil_liberties_index import CivilLibertiesIndex
from app.layers.political.conflict import ConflictEconomics
from app.layers.political.corruption import CorruptionEconomics
from app.layers.political.democratization_trend import DemocratizationTrend
from app.layers.political.election_economics import ElectionEconomics
from app.layers.political.electoral_integrity import ElectoralIntegrity
from app.layers.political.government_stability import GovernmentStability
from app.layers.political.lobbying import LobbyingEconomics
from app.layers.political.media_economics import MediaEconomics
from app.layers.political.policy_credibility import PolicyCredibility
from app.layers.political.political_business_cycle import PoliticalBusinessCycle
from app.layers.political.regime_stability import RegimeStability
from app.layers.political.regulatory_capture import RegulatoryCapture
from app.layers.political.resource_politics import ResourcePolitics
from app.layers.political.sanctions import SanctionsEconomics
from app.layers.political.state_capacity import StateCapacity
from app.layers.political.trade_war import TradeWarAnalysis
from app.layers.political.veto_players_index import VetoPlayersIndex

ALL_MODULES = [
    PoliticalBusinessCycle,
    LobbyingEconomics,
    CorruptionEconomics,
    ConflictEconomics,
    SanctionsEconomics,
    TradeWarAnalysis,
    MediaEconomics,
    ElectionEconomics,
    StateCapacity,
    RegulatoryCapture,
    RegimeStability,
    CivilLibertiesIndex,
    AnticorruptionEffectiveness,
    ElectoralIntegrity,
    PolicyCredibility,
    GovernmentStability,
    VetoPlayersIndex,
    ResourcePolitics,
    DemocratizationTrend,
]

__all__ = [
    "PoliticalBusinessCycle",
    "LobbyingEconomics",
    "CorruptionEconomics",
    "ConflictEconomics",
    "SanctionsEconomics",
    "TradeWarAnalysis",
    "MediaEconomics",
    "ElectionEconomics",
    "StateCapacity",
    "RegulatoryCapture",
    "RegimeStability",
    "CivilLibertiesIndex",
    "AnticorruptionEffectiveness",
    "ElectoralIntegrity",
    "PolicyCredibility",
    "GovernmentStability",
    "VetoPlayersIndex",
    "ResourcePolitics",
    "DemocratizationTrend",
    "ALL_MODULES",
]
