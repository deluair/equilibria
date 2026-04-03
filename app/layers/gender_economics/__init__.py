from app.layers.gender_economics.female_labor_participation import FemaleLabourParticipation
from app.layers.gender_economics.gender_development_index import GenderDevelopmentIndex
from app.layers.gender_economics.gender_education_gap import GenderEducationGap
from app.layers.gender_economics.gender_financial_inclusion import GenderFinancialInclusion
from app.layers.gender_economics.gender_legal_rights import GenderLegalRights
from app.layers.gender_economics.gender_poverty_gap import GenderPovertyGap
from app.layers.gender_economics.gender_time_use_burden import GenderTimeUseBurden
from app.layers.gender_economics.gender_wage_gap import GenderWageGap
from app.layers.gender_economics.maternal_mortality_economics import MaternalMortalityEconomics
from app.layers.gender_economics.women_in_leadership import WomenInLeadership

ALL_MODULES = [
    GenderWageGap,
    FemaleLabourParticipation,
    GenderEducationGap,
    WomenInLeadership,
    GenderTimeUseBurden,
    MaternalMortalityEconomics,
    GenderFinancialInclusion,
    GenderLegalRights,
    GenderPovertyGap,
    GenderDevelopmentIndex,
]

__all__ = [
    "GenderWageGap",
    "FemaleLabourParticipation",
    "GenderEducationGap",
    "WomenInLeadership",
    "GenderTimeUseBurden",
    "MaternalMortalityEconomics",
    "GenderFinancialInclusion",
    "GenderLegalRights",
    "GenderPovertyGap",
    "GenderDevelopmentIndex",
    "ALL_MODULES",
]
