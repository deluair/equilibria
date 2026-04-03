from app.layers.income_distribution.asset_income_inequality import AssetIncomeInequality
from app.layers.income_distribution.disposable_income_trend import DisposableIncomeTrend
from app.layers.income_distribution.functional_income_distribution import FunctionalIncomeDistribution
from app.layers.income_distribution.household_income_volatility import HouseholdIncomeVolatility
from app.layers.income_distribution.income_floor_adequacy import IncomeFloorAdequacy
from app.layers.income_distribution.middle_class_share import MiddleClassShare
from app.layers.income_distribution.poverty_gap_depth import PovertyGapDepth
from app.layers.income_distribution.quintile_income_ratio import QuintileIncomeRatio
from app.layers.income_distribution.transfers_redistribution import TransfersRedistribution
from app.layers.income_distribution.wage_growth_gap import WageGrowthGap

ALL_MODULES = [
    FunctionalIncomeDistribution,
    QuintileIncomeRatio,
    MiddleClassShare,
    WageGrowthGap,
    HouseholdIncomeVolatility,
    PovertyGapDepth,
    TransfersRedistribution,
    DisposableIncomeTrend,
    IncomeFloorAdequacy,
    AssetIncomeInequality,
]

__all__ = [
    "FunctionalIncomeDistribution",
    "QuintileIncomeRatio",
    "MiddleClassShare",
    "WageGrowthGap",
    "HouseholdIncomeVolatility",
    "PovertyGapDepth",
    "TransfersRedistribution",
    "DisposableIncomeTrend",
    "IncomeFloorAdequacy",
    "AssetIncomeInequality",
    "ALL_MODULES",
]
