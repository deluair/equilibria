from app.layers.labor.automation_exposure import AutomationExposure
from app.layers.labor.beveridge_curve import BeveridgeCurve
from app.layers.labor.child_labor import ChildLabor
from app.layers.labor.employment_vulnerability import EmploymentVulnerability
from app.layers.labor.gig_economy import GigEconomy
from app.layers.labor.job_polarization import JobPolarization
from app.layers.labor.labor_force import LaborForceParticipation
from app.layers.labor.labor_force_participation import LaborForceParticipationGap
from app.layers.labor.labor_market_efficiency import LaborMarketEfficiency
from app.layers.labor.labor_productivity_growth import LaborProductivityGrowth
from app.layers.labor.labor_rights_index import LaborRightsIndex
from app.layers.labor.labor_tightness import LaborMarketTightness
from app.layers.labor.migration_gravity import MigrationGravity
from app.layers.labor.mincer import MincerWageEquation
from app.layers.labor.minimum_wage import MinimumWageEffects
from app.layers.labor.oaxaca_blinder import OaxacaBlinder
from app.layers.labor.part_time_employment import PartTimeEmployment
from app.layers.labor.remittance import RemittanceDeterminants
from app.layers.labor.remote_work import RemoteWork
from app.layers.labor.returns_education import ReturnsToEducation
from app.layers.labor.sectoral_reallocation import SectoralReallocation
from app.layers.labor.self_employment_rate import SelfEmploymentRate
from app.layers.labor.shift_share import ShiftShareAnalysis
from app.layers.labor.skill_premium import SkillPremium
from app.layers.labor.skills_mismatch import SkillsMismatch
from app.layers.labor.unemployment_duration import UnemploymentDuration
from app.layers.labor.union_premium import UnionWagePremium
from app.layers.labor.wage_inequality import WageInequality
from app.layers.labor.wage_phillips import WagePhillipsCurve
from app.layers.labor.youth_unemployment import YouthUnemployment

ALL_MODULES = [
    MincerWageEquation,
    OaxacaBlinder,
    ReturnsToEducation,
    MigrationGravity,
    RemittanceDeterminants,
    UnemploymentDuration,
    BeveridgeCurve,
    ShiftShareAnalysis,
    LaborForceParticipation,
    SkillPremium,
    WagePhillipsCurve,
    UnionWagePremium,
    MinimumWageEffects,
    AutomationExposure,
    LaborMarketTightness,
    SectoralReallocation,
    GigEconomy,
    RemoteWork,
    SkillsMismatch,
    JobPolarization,
    YouthUnemployment,
    LaborProductivityGrowth,
    SelfEmploymentRate,
    WageInequality,
    LaborForceParticipationGap,
    ChildLabor,
    EmploymentVulnerability,
    LaborMarketEfficiency,
    PartTimeEmployment,
    LaborRightsIndex,
]

__all__ = [
    "MincerWageEquation",
    "OaxacaBlinder",
    "ReturnsToEducation",
    "MigrationGravity",
    "RemittanceDeterminants",
    "UnemploymentDuration",
    "BeveridgeCurve",
    "ShiftShareAnalysis",
    "LaborForceParticipation",
    "SkillPremium",
    "WagePhillipsCurve",
    "UnionWagePremium",
    "MinimumWageEffects",
    "AutomationExposure",
    "LaborMarketTightness",
    "SectoralReallocation",
    "GigEconomy",
    "RemoteWork",
    "SkillsMismatch",
    "JobPolarization",
    "YouthUnemployment",
    "LaborProductivityGrowth",
    "SelfEmploymentRate",
    "WageInequality",
    "LaborForceParticipationGap",
    "ChildLabor",
    "EmploymentVulnerability",
    "LaborMarketEfficiency",
    "PartTimeEmployment",
    "LaborRightsIndex",
    "ALL_MODULES",
]
