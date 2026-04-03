from app.layers.education_economics.education_expenditure_efficiency import EducationExpenditureEfficiency
from app.layers.education_economics.education_inequality import EducationInequality
from app.layers.education_economics.education_mobility_index import EducationMobilityIndex
from app.layers.education_economics.education_quality_score import EducationQualityScore
from app.layers.education_economics.human_capital_stock import HumanCapitalStock
from app.layers.education_economics.returns_to_education import ReturnsToEducation
from app.layers.education_economics.school_enrollment_gap import SchoolEnrollmentGap
from app.layers.education_economics.skill_mismatch_index import SkillMismatchIndex
from app.layers.education_economics.stem_graduate_share import StemGraduateShare
from app.layers.education_economics.tertiary_expansion_rate import TertiaryExpansionRate

ALL_MODULES = [
    ReturnsToEducation,
    EducationExpenditureEfficiency,
    SchoolEnrollmentGap,
    HumanCapitalStock,
    EducationInequality,
    SkillMismatchIndex,
    TertiaryExpansionRate,
    EducationQualityScore,
    StemGraduateShare,
    EducationMobilityIndex,
]

__all__ = [
    "ReturnsToEducation",
    "EducationExpenditureEfficiency",
    "SchoolEnrollmentGap",
    "HumanCapitalStock",
    "EducationInequality",
    "SkillMismatchIndex",
    "TertiaryExpansionRate",
    "EducationQualityScore",
    "StemGraduateShare",
    "EducationMobilityIndex",
    "ALL_MODULES",
]
