from app.layers.social_protection.aging_social_burden import AgingSocialBurden
from app.layers.social_protection.child_benefit_coverage import ChildBenefitCoverage
from app.layers.social_protection.disability_inclusion import DisabilityInclusion
from app.layers.social_protection.healthcare_universality import HealthcareUniversality
from app.layers.social_protection.informal_economy_protection import InformalEconomyProtection
from app.layers.social_protection.minimum_wage_adequacy import MinimumWageAdequacy
from app.layers.social_protection.pension_adequacy import PensionAdequacy
from app.layers.social_protection.social_insurance_coverage import SocialInsuranceCoverage
from app.layers.social_protection.social_spending_adequacy import SocialSpendingAdequacy
from app.layers.social_protection.unemployment_protection import UnemploymentProtection

ALL_MODULES = [
    SocialInsuranceCoverage,
    PensionAdequacy,
    UnemploymentProtection,
    HealthcareUniversality,
    ChildBenefitCoverage,
    SocialSpendingAdequacy,
    InformalEconomyProtection,
    DisabilityInclusion,
    AgingSocialBurden,
    MinimumWageAdequacy,
]

__all__ = [
    "SocialInsuranceCoverage",
    "PensionAdequacy",
    "UnemploymentProtection",
    "HealthcareUniversality",
    "ChildBenefitCoverage",
    "SocialSpendingAdequacy",
    "InformalEconomyProtection",
    "DisabilityInclusion",
    "AgingSocialBurden",
    "MinimumWageAdequacy",
    "ALL_MODULES",
]
