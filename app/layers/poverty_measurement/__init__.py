from app.layers.poverty_measurement.headcount_poverty_index import HeadcountPovertyIndex
from app.layers.poverty_measurement.poverty_gap_index import PovertyGapIndex
from app.layers.poverty_measurement.poverty_severity_index import PovertyGapSqIndex
from app.layers.poverty_measurement.multidimensional_poverty import MultidimensionalPoverty
from app.layers.poverty_measurement.relative_poverty_rate import RelativePovertyRate
from app.layers.poverty_measurement.poverty_trap_indicator import PovertyTrapIndicator
from app.layers.poverty_measurement.child_poverty_rate import ChildPovertyRate
from app.layers.poverty_measurement.working_poor_index import WorkingPoorIndex
from app.layers.poverty_measurement.social_transfer_adequacy import SocialTransferAdequacy
from app.layers.poverty_measurement.poverty_convergence_rate import PovertyConvergenceRate

ALL_MODULES = [
    HeadcountPovertyIndex,
    PovertyGapIndex,
    PovertyGapSqIndex,
    MultidimensionalPoverty,
    RelativePovertyRate,
    PovertyTrapIndicator,
    ChildPovertyRate,
    WorkingPoorIndex,
    SocialTransferAdequacy,
    PovertyConvergenceRate,
]

__all__ = [
    "HeadcountPovertyIndex",
    "PovertyGapIndex",
    "PovertyGapSqIndex",
    "MultidimensionalPoverty",
    "RelativePovertyRate",
    "PovertyTrapIndicator",
    "ChildPovertyRate",
    "WorkingPoorIndex",
    "SocialTransferAdequacy",
    "PovertyConvergenceRate",
    "ALL_MODULES",
]
