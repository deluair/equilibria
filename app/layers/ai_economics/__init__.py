from app.layers.ai_economics.ai_job_displacement_risk import AIJobDisplacementRisk
from app.layers.ai_economics.ai_productivity_dividend import AIProductivityDividend
from app.layers.ai_economics.ai_investment_concentration import AIInvestmentConcentration
from app.layers.ai_economics.ai_regulation_burden import AIRegulationBurden
from app.layers.ai_economics.algorithmic_bias_economics import AlgorithmicBiasEconomics
from app.layers.ai_economics.ai_compute_concentration import AIComputeConcentration
from app.layers.ai_economics.ai_startup_ecosystem import AIStartupEcosystem
from app.layers.ai_economics.automation_income_polarization import AutomationIncomePolarization
from app.layers.ai_economics.ai_trade_competitiveness import AITradeCompetitiveness
from app.layers.ai_economics.generative_ai_market_impact import GenerativeAIMarketImpact

ALL_MODULES = [
    AIJobDisplacementRisk,
    AIProductivityDividend,
    AIInvestmentConcentration,
    AIRegulationBurden,
    AlgorithmicBiasEconomics,
    AIComputeConcentration,
    AIStartupEcosystem,
    AutomationIncomePolarization,
    AITradeCompetitiveness,
    GenerativeAIMarketImpact,
]

__all__ = [
    "AIJobDisplacementRisk",
    "AIProductivityDividend",
    "AIInvestmentConcentration",
    "AIRegulationBurden",
    "AlgorithmicBiasEconomics",
    "AIComputeConcentration",
    "AIStartupEcosystem",
    "AutomationIncomePolarization",
    "AITradeCompetitiveness",
    "GenerativeAIMarketImpact",
    "ALL_MODULES",
]
