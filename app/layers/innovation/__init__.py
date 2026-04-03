from app.layers.innovation.creative_destruction import CreativeDestruction
from app.layers.innovation.digital_transformation import DigitalTransformation
from app.layers.innovation.innovation_ecosystem import InnovationEcosystem
from app.layers.innovation.innovation_efficiency import InnovationEfficiency
from app.layers.innovation.knowledge_diffusion import KnowledgeDiffusion
from app.layers.innovation.knowledge_economy_index import KnowledgeEconomyIndex
from app.layers.innovation.national_innovation_system import NationalInnovationSystem
from app.layers.innovation.open_innovation import OpenInnovation
from app.layers.innovation.stem_capacity import STEMCapacity
from app.layers.innovation.technological_readiness import TechnologicalReadiness

ALL_MODULES = [
    NationalInnovationSystem,
    KnowledgeEconomyIndex,
    TechnologicalReadiness,
    InnovationEfficiency,
    KnowledgeDiffusion,
    STEMCapacity,
    OpenInnovation,
    InnovationEcosystem,
    DigitalTransformation,
    CreativeDestruction,
]

__all__ = [
    "NationalInnovationSystem",
    "KnowledgeEconomyIndex",
    "TechnologicalReadiness",
    "InnovationEfficiency",
    "KnowledgeDiffusion",
    "STEMCapacity",
    "OpenInnovation",
    "InnovationEcosystem",
    "DigitalTransformation",
    "CreativeDestruction",
    "ALL_MODULES",
]
