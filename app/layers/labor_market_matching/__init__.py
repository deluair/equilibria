from app.layers.labor_market_matching.search_friction_index import SearchFrictionIndex
from app.layers.labor_market_matching.skill_mismatch_rate import SkillMismatchRate
from app.layers.labor_market_matching.job_vacancy_fill_time import JobVacancyFillTime
from app.layers.labor_market_matching.recruitment_cost_burden import RecruitmentCostBurden
from app.layers.labor_market_matching.matching_efficiency_index import MatchingEfficiencyIndex
from app.layers.labor_market_matching.geographic_mobility_gap import GeographicMobilityGap
from app.layers.labor_market_matching.online_platform_matching_quality import OnlinePlatformMatchingQuality
from app.layers.labor_market_matching.worker_retention_rate import WorkerRetentionRate
from app.layers.labor_market_matching.labor_market_polarization_trend import LaborMarketPolarizationTrend
from app.layers.labor_market_matching.remote_work_matching_expansion import RemoteWorkMatchingExpansion

ALL_MODULES = [
    SearchFrictionIndex,
    SkillMismatchRate,
    JobVacancyFillTime,
    RecruitmentCostBurden,
    MatchingEfficiencyIndex,
    GeographicMobilityGap,
    OnlinePlatformMatchingQuality,
    WorkerRetentionRate,
    LaborMarketPolarizationTrend,
    RemoteWorkMatchingExpansion,
]

__all__ = [
    "SearchFrictionIndex",
    "SkillMismatchRate",
    "JobVacancyFillTime",
    "RecruitmentCostBurden",
    "MatchingEfficiencyIndex",
    "GeographicMobilityGap",
    "OnlinePlatformMatchingQuality",
    "WorkerRetentionRate",
    "LaborMarketPolarizationTrend",
    "RemoteWorkMatchingExpansion",
    "ALL_MODULES",
]
