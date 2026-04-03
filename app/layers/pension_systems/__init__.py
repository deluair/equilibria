from app.layers.pension_systems.aging_dependency_ratio import AgingDependencyRatio
from app.layers.pension_systems.demographic_pension_pressure import DemographicPensionPressure
from app.layers.pension_systems.funded_vs_unfunded_risk import FundedVsUnfundedRisk
from app.layers.pension_systems.intergenerational_equity import IntergenerationalEquity
from app.layers.pension_systems.pension_coverage import PensionCoverage
from app.layers.pension_systems.pension_fiscal_sustainability import PensionFiscalSustainability
from app.layers.pension_systems.pension_poverty_gap import PensionPovertyGap
from app.layers.pension_systems.pension_reform_urgency import PensionReformUrgency
from app.layers.pension_systems.pension_replacement_rate import PensionReplacementRate
from app.layers.pension_systems.retirement_age_gap import RetirementAgeGap

ALL_MODULES = [
    PensionCoverage,
    AgingDependencyRatio,
    PensionFiscalSustainability,
    RetirementAgeGap,
    PensionReplacementRate,
    DemographicPensionPressure,
    FundedVsUnfundedRisk,
    PensionPovertyGap,
    IntergenerationalEquity,
    PensionReformUrgency,
]

__all__ = [
    "PensionCoverage",
    "AgingDependencyRatio",
    "PensionFiscalSustainability",
    "RetirementAgeGap",
    "PensionReplacementRate",
    "DemographicPensionPressure",
    "FundedVsUnfundedRisk",
    "PensionPovertyGap",
    "IntergenerationalEquity",
    "PensionReformUrgency",
    "ALL_MODULES",
]
