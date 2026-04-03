from app.layers.labor.automation_exposure import AutomationExposure
from app.layers.labor.beveridge_curve import BeveridgeCurve
from app.layers.labor.labor_force import LaborForceParticipation
from app.layers.labor.labor_tightness import LaborMarketTightness
from app.layers.labor.migration_gravity import MigrationGravity
from app.layers.labor.mincer import MincerWageEquation
from app.layers.labor.minimum_wage import MinimumWageEffects
from app.layers.labor.oaxaca_blinder import OaxacaBlinder
from app.layers.labor.remittance import RemittanceDeterminants
from app.layers.labor.returns_education import ReturnsToEducation
from app.layers.labor.sectoral_reallocation import SectoralReallocation
from app.layers.labor.shift_share import ShiftShareAnalysis
from app.layers.labor.skill_premium import SkillPremium
from app.layers.labor.unemployment_duration import UnemploymentDuration
from app.layers.labor.union_premium import UnionWagePremium
from app.layers.labor.wage_phillips import WagePhillipsCurve

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
    "ALL_MODULES",
]
