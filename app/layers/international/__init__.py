from app.layers.international.currency_wars import CurrencyWars
from app.layers.international.diplomatic_trade_links import DiplomaticTradeLinks
from app.layers.international.dollar_dominance import DollarDominance
from app.layers.international.foreign_aid_effectiveness import ForeignAidEffectiveness
from app.layers.international.geopolitical_risk_index import GeopoliticalRiskIndex
from app.layers.international.global_imbalances import GlobalImbalances
from app.layers.international.imf_program_effects import IMFProgramEffects
from app.layers.international.multilateral_negotiations import MultilateralNegotiations
from app.layers.international.regional_integration import RegionalIntegration
from app.layers.international.sanctions_impact import SanctionsImpact

ALL_MODULES = [
    ForeignAidEffectiveness,
    DollarDominance,
    GlobalImbalances,
    GeopoliticalRiskIndex,
    DiplomaticTradeLinks,
    SanctionsImpact,
    RegionalIntegration,
    CurrencyWars,
    MultilateralNegotiations,
    IMFProgramEffects,
]

__all__ = [
    "ForeignAidEffectiveness",
    "DollarDominance",
    "GlobalImbalances",
    "GeopoliticalRiskIndex",
    "DiplomaticTradeLinks",
    "SanctionsImpact",
    "RegionalIntegration",
    "CurrencyWars",
    "MultilateralNegotiations",
    "IMFProgramEffects",
    "ALL_MODULES",
]
