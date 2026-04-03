from app.layers.welfare.atkinson_inequality import AtkinsonInequality
from app.layers.welfare.capabilities_index import CapabilitiesIndex
from app.layers.welfare.intergenerational_mobility import IntergenerationalMobility
from app.layers.welfare.living_standards import LivingStandards
from app.layers.welfare.multidimensional_poverty import MultidimensionalPoverty
from app.layers.welfare.poverty_decomposition import PovertyDecomposition
from app.layers.welfare.redistribution_analysis import RedistributionAnalysis
from app.layers.welfare.social_exclusion import SocialExclusion
from app.layers.welfare.social_welfare_function import SocialWelfareFunction
from app.layers.welfare.subjective_wellbeing import SubjectiveWellbeing

ALL_MODULES = [
    AtkinsonInequality,
    PovertyDecomposition,
    CapabilitiesIndex,
    SubjectiveWellbeing,
    LivingStandards,
    RedistributionAnalysis,
    SocialExclusion,
    MultidimensionalPoverty,
    IntergenerationalMobility,
    SocialWelfareFunction,
]

__all__ = [
    "AtkinsonInequality",
    "PovertyDecomposition",
    "CapabilitiesIndex",
    "SubjectiveWellbeing",
    "LivingStandards",
    "RedistributionAnalysis",
    "SocialExclusion",
    "MultidimensionalPoverty",
    "IntergenerationalMobility",
    "SocialWelfareFunction",
    "ALL_MODULES",
]
