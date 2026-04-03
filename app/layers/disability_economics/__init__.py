from app.layers.disability_economics.disability_employment_gap import DisabilityEmploymentGap
from app.layers.disability_economics.disability_income_penalty import DisabilityIncomePenalty
from app.layers.disability_economics.accessibility_infrastructure_gap import AccessibilityInfrastructureGap
from app.layers.disability_economics.ada_compliance_cost import ADAComplianceCost
from app.layers.disability_economics.assistive_technology_market import AssistiveTechnologyMarket
from app.layers.disability_economics.disability_social_protection import DisabilitySocialProtection
from app.layers.disability_economics.inclusive_education_economics import InclusiveEducationEconomics
from app.layers.disability_economics.caregiver_economic_burden import CaregiverEconomicBurden
from app.layers.disability_economics.disability_poverty_nexus import DisabilityPovertyNexus
from app.layers.disability_economics.universal_design_investment import UniversalDesignInvestment

ALL_MODULES = [
    DisabilityEmploymentGap,
    DisabilityIncomePenalty,
    AccessibilityInfrastructureGap,
    ADAComplianceCost,
    AssistiveTechnologyMarket,
    DisabilitySocialProtection,
    InclusiveEducationEconomics,
    CaregiverEconomicBurden,
    DisabilityPovertyNexus,
    UniversalDesignInvestment,
]

__all__ = [
    "DisabilityEmploymentGap",
    "DisabilityIncomePenalty",
    "AccessibilityInfrastructureGap",
    "ADAComplianceCost",
    "AssistiveTechnologyMarket",
    "DisabilitySocialProtection",
    "InclusiveEducationEconomics",
    "CaregiverEconomicBurden",
    "DisabilityPovertyNexus",
    "UniversalDesignInvestment",
    "ALL_MODULES",
]
