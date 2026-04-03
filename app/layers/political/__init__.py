from app.layers.political.conflict import ConflictEconomics
from app.layers.political.corruption import CorruptionEconomics
from app.layers.political.lobbying import LobbyingEconomics
from app.layers.political.political_business_cycle import PoliticalBusinessCycle
from app.layers.political.sanctions import SanctionsEconomics
from app.layers.political.trade_war import TradeWarAnalysis

ALL_MODULES = [
    PoliticalBusinessCycle,
    LobbyingEconomics,
    CorruptionEconomics,
    ConflictEconomics,
    SanctionsEconomics,
    TradeWarAnalysis,
]

__all__ = [
    "PoliticalBusinessCycle",
    "LobbyingEconomics",
    "CorruptionEconomics",
    "ConflictEconomics",
    "SanctionsEconomics",
    "TradeWarAnalysis",
    "ALL_MODULES",
]
