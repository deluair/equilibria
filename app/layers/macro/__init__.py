from app.layers.macro.fci import FinancialConditionsIndex
from app.layers.macro.credit_impulse import CreditImpulse
from app.layers.macro.yield_curve import YieldCurve
from app.layers.macro.inflation_decomposition import InflationDecomposition
from app.layers.macro.monetary_transmission import MonetaryTransmission
from app.layers.macro.output_gap import OutputGap
from app.layers.macro.structural_break import StructuralBreak
from app.layers.macro.recession_probability import RecessionProbability
from app.layers.macro.nowcasting import Nowcasting
from app.layers.macro.var_irf import VARImpulseResponse

ALL_MODULES = [
    FinancialConditionsIndex,
    CreditImpulse,
    YieldCurve,
    InflationDecomposition,
    MonetaryTransmission,
    OutputGap,
    StructuralBreak,
    RecessionProbability,
    Nowcasting,
    VARImpulseResponse,
]

__all__ = [
    "FinancialConditionsIndex",
    "CreditImpulse",
    "YieldCurve",
    "InflationDecomposition",
    "MonetaryTransmission",
    "OutputGap",
    "StructuralBreak",
    "RecessionProbability",
    "Nowcasting",
    "VARImpulseResponse",
    "ALL_MODULES",
]
