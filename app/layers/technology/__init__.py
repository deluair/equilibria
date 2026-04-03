from app.layers.technology.automation_labor_impact import AutomationLaborImpact
from app.layers.technology.digital_economy import DigitalEconomy
from app.layers.technology.innovation_index import InnovationIndex
from app.layers.technology.knowledge_spillovers import KnowledgeSpillovers
from app.layers.technology.network_effects import NetworkEffects
from app.layers.technology.patent_economics import PatentEconomics
from app.layers.technology.platform_economics import PlatformEconomics
from app.layers.technology.rnd_returns import RnDReturns
from app.layers.technology.technology_diffusion import TechnologyDiffusion
from app.layers.technology.tfp_estimation import TFPEstimation

ALL_MODULES = [
    TFPEstimation,
    InnovationIndex,
    DigitalEconomy,
    AutomationLaborImpact,
    TechnologyDiffusion,
    RnDReturns,
    KnowledgeSpillovers,
    NetworkEffects,
    PlatformEconomics,
    PatentEconomics,
]

__all__ = [
    "TFPEstimation",
    "InnovationIndex",
    "DigitalEconomy",
    "AutomationLaborImpact",
    "TechnologyDiffusion",
    "RnDReturns",
    "KnowledgeSpillovers",
    "NetworkEffects",
    "PlatformEconomics",
    "PatentEconomics",
    "ALL_MODULES",
]
