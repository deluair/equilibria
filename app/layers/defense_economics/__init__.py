from app.layers.defense_economics.defense_budget_efficiency import DefenseBudgetEfficiency
from app.layers.defense_economics.military_spending_growth_impact import MilitarySpendingGrowthImpact
from app.layers.defense_economics.arms_trade_balance import ArmsTradeBalance
from app.layers.defense_economics.defense_industry_multiplier import DefenseIndustryMultiplier
from app.layers.defense_economics.veteran_economic_integration import VeteranEconomicIntegration
from app.layers.defense_economics.defense_rnd_spillover import DefenseRndSpillover
from app.layers.defense_economics.security_economic_stability import SecurityEconomicStability
from app.layers.defense_economics.conflict_economic_cost import ConflictEconomicCost
from app.layers.defense_economics.nuclear_deterrence_economics import NuclearDeterrenceEconomics
from app.layers.defense_economics.military_labor_market import MilitaryLaborMarket

ALL_MODULES = [
    DefenseBudgetEfficiency,
    MilitarySpendingGrowthImpact,
    ArmsTradeBalance,
    DefenseIndustryMultiplier,
    VeteranEconomicIntegration,
    DefenseRndSpillover,
    SecurityEconomicStability,
    ConflictEconomicCost,
    NuclearDeterrenceEconomics,
    MilitaryLaborMarket,
]

__all__ = [
    "DefenseBudgetEfficiency",
    "MilitarySpendingGrowthImpact",
    "ArmsTradeBalance",
    "DefenseIndustryMultiplier",
    "VeteranEconomicIntegration",
    "DefenseRndSpillover",
    "SecurityEconomicStability",
    "ConflictEconomicCost",
    "NuclearDeterrenceEconomics",
    "MilitaryLaborMarket",
    "ALL_MODULES",
]
