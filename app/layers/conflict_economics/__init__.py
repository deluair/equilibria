from app.layers.conflict_economics.conflict_gdp_loss import ConflictGdpLoss
from app.layers.conflict_economics.reconstruction_cost import ReconstructionCost
from app.layers.conflict_economics.refugee_economic_burden import RefugeeEconomicBurden
from app.layers.conflict_economics.fragility_index import FragilityIndex
from app.layers.conflict_economics.war_trade_disruption import WarTradeDisruption
from app.layers.conflict_economics.conflict_investment_chill import ConflictInvestmentChill
from app.layers.conflict_economics.post_conflict_recovery import PostConflictRecovery
from app.layers.conflict_economics.conflict_poverty_nexus import ConflictPovertyNexus
from app.layers.conflict_economics.arms_spending_opportunity import ArmsSpendingOpportunity
from app.layers.conflict_economics.peacebuilding_returns import PeacebuildingReturns

ALL_MODULES = [
    ConflictGdpLoss,
    ReconstructionCost,
    RefugeeEconomicBurden,
    FragilityIndex,
    WarTradeDisruption,
    ConflictInvestmentChill,
    PostConflictRecovery,
    ConflictPovertyNexus,
    ArmsSpendingOpportunity,
    PeacebuildingReturns,
]

__all__ = [
    "ConflictGdpLoss",
    "ReconstructionCost",
    "RefugeeEconomicBurden",
    "FragilityIndex",
    "WarTradeDisruption",
    "ConflictInvestmentChill",
    "PostConflictRecovery",
    "ConflictPovertyNexus",
    "ArmsSpendingOpportunity",
    "PeacebuildingReturns",
    "ALL_MODULES",
]
