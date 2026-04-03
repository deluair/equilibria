from app.layers.law_economics.legal_system_efficiency import LegalSystemEfficiency
from app.layers.law_economics.property_rights_protection import PropertyRightsProtection
from app.layers.law_economics.judicial_independence_index import JudicalIndependenceIndex
from app.layers.law_economics.contract_cost_index import ContractCostIndex
from app.layers.law_economics.ip_protection_strength import IpProtectionStrength
from app.layers.law_economics.legal_access_equity import LegalAccessEquity
from app.layers.law_economics.corruption_rule_of_law import CorruptionRuleOfLaw
from app.layers.law_economics.corporate_law_quality import CorporateLawQuality
from app.layers.law_economics.legal_formalism_index import LegalFormalismIndex
from app.layers.law_economics.law_development_nexus import LawDevelopmentNexus

ALL_MODULES = [
    LegalSystemEfficiency,
    PropertyRightsProtection,
    JudicalIndependenceIndex,
    ContractCostIndex,
    IpProtectionStrength,
    LegalAccessEquity,
    CorruptionRuleOfLaw,
    CorporateLawQuality,
    LegalFormalismIndex,
    LawDevelopmentNexus,
]

__all__ = [
    "LegalSystemEfficiency",
    "PropertyRightsProtection",
    "JudicalIndependenceIndex",
    "ContractCostIndex",
    "IpProtectionStrength",
    "LegalAccessEquity",
    "CorruptionRuleOfLaw",
    "CorporateLawQuality",
    "LegalFormalismIndex",
    "LawDevelopmentNexus",
    "ALL_MODULES",
]
