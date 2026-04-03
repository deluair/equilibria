from app.layers.macro.business_cycle import BusinessCycle
from app.layers.macro.credit_impulse import CreditImpulse
from app.layers.macro.debt_sustainability import DebtSustainability
from app.layers.macro.erpt import ExchangeRatePassThrough
from app.layers.macro.gdp_decomposition import GDPDecomposition
from app.layers.macro.inflation_expectations import InflationExpectations
from app.layers.macro.macro_uncertainty import MacroUncertainty
from app.layers.macro.phillips_curve import PhillipsCurve
from app.layers.macro.taylor_rule import TaylorRule
from app.layers.macro.dsge_calibration import DSGECalibration
from app.layers.macro.dsge_estimation import DSGEEstimation
from app.layers.macro.fci import FinancialConditionsIndex
from app.layers.macro.fiscal_space import FiscalSpace
from app.layers.macro.global_var import GlobalVAR
from app.layers.macro.hysteresis import Hysteresis
from app.layers.macro.inflation_decomposition import InflationDecomposition
from app.layers.macro.monetary_transmission import MonetaryTransmission
from app.layers.macro.nowcasting import Nowcasting
from app.layers.macro.output_gap import OutputGap
from app.layers.macro.real_interest_rate import RealInterestRate
from app.layers.macro.recession_probability import RecessionProbability
from app.layers.macro.regime_switching import RegimeSwitching
from app.layers.macro.secular_stagnation import SecularStagnation
from app.layers.macro.shadow_banking import ShadowBanking
from app.layers.macro.structural_break import StructuralBreak
from app.layers.macro.twin_deficits import TwinDeficits
from app.layers.macro.var_irf import VARImpulseResponse
from app.layers.macro.wage_price_spiral import WagePriceSpiral
from app.layers.macro.yield_curve import YieldCurve

ALL_MODULES = [
    GDPDecomposition,
    PhillipsCurve,
    TaylorRule,
    BusinessCycle,
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
    DSGECalibration,
    RegimeSwitching,
    GlobalVAR,
    RealInterestRate,
    WagePriceSpiral,
    FiscalSpace,
    DSGEEstimation,
    ShadowBanking,
    SecularStagnation,
    Hysteresis,
    DebtSustainability,
    TwinDeficits,
    ExchangeRatePassThrough,
    InflationExpectations,
    MacroUncertainty,
]

__all__ = [
    "BusinessCycle",
    "GDPDecomposition",
    "PhillipsCurve",
    "TaylorRule",
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
    "DSGECalibration",
    "RegimeSwitching",
    "GlobalVAR",
    "RealInterestRate",
    "WagePriceSpiral",
    "FiscalSpace",
    "DSGEEstimation",
    "ShadowBanking",
    "SecularStagnation",
    "Hysteresis",
    "DebtSustainability",
    "TwinDeficits",
    "ExchangeRatePassThrough",
    "InflationExpectations",
    "MacroUncertainty",
    "ALL_MODULES",
]
