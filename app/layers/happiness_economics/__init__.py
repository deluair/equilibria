from app.layers.happiness_economics.subjective_wellbeing_gdp_gap import SubjectiveWellbeingGDPGap
from app.layers.happiness_economics.income_happiness_threshold import IncomeHappinessThreshold
from app.layers.happiness_economics.inequality_wellbeing_penalty import InequalityWellbeingPenalty
from app.layers.happiness_economics.social_trust_happiness_dividend import SocialTrustHappinessDividend
from app.layers.happiness_economics.work_life_balance_index import WorkLifeBalanceIndex
from app.layers.happiness_economics.environmental_wellbeing_link import EnvironmentalWellbeingLink
from app.layers.happiness_economics.mental_health_economic_burden import MentalHealthEconomicBurden
from app.layers.happiness_economics.happiness_policy_effectiveness import HappinessPolicyEffectiveness
from app.layers.happiness_economics.gdp_beyond_measure_gap import GDPBeyondMeasureGap
from app.layers.happiness_economics.loneliness_economic_cost import LonelinessEconomicCost

ALL_MODULES = [
    SubjectiveWellbeingGDPGap,
    IncomeHappinessThreshold,
    InequalityWellbeingPenalty,
    SocialTrustHappinessDividend,
    WorkLifeBalanceIndex,
    EnvironmentalWellbeingLink,
    MentalHealthEconomicBurden,
    HappinessPolicyEffectiveness,
    GDPBeyondMeasureGap,
    LonelinessEconomicCost,
]

__all__ = [
    "SubjectiveWellbeingGDPGap",
    "IncomeHappinessThreshold",
    "InequalityWellbeingPenalty",
    "SocialTrustHappinessDividend",
    "WorkLifeBalanceIndex",
    "EnvironmentalWellbeingLink",
    "MentalHealthEconomicBurden",
    "HappinessPolicyEffectiveness",
    "GDPBeyondMeasureGap",
    "LonelinessEconomicCost",
    "ALL_MODULES",
]
