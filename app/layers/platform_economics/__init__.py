from app.layers.platform_economics.platform_market_dominance import PlatformMarketDominance
from app.layers.platform_economics.network_effects_intensity import NetworkEffectsIntensity
from app.layers.platform_economics.gig_worker_vulnerability import GigWorkerVulnerability
from app.layers.platform_economics.data_monopoly_risk import DataMonopolyRisk
from app.layers.platform_economics.platform_tax_erosion import PlatformTaxErosion
from app.layers.platform_economics.winner_takes_all_dynamics import WinnerTakesAllDynamics
from app.layers.platform_economics.platform_labor_regulation_gap import PlatformLaborRegulationGap
from app.layers.platform_economics.digital_antitrust_capacity import DigitalAntitrustCapacity
from app.layers.platform_economics.platform_financial_inclusion import PlatformFinancialInclusion
from app.layers.platform_economics.algorithmic_pricing_risk import AlgorithmicPricingRisk

ALL_MODULES = [
    PlatformMarketDominance,
    NetworkEffectsIntensity,
    GigWorkerVulnerability,
    DataMonopolyRisk,
    PlatformTaxErosion,
    WinnerTakesAllDynamics,
    PlatformLaborRegulationGap,
    DigitalAntitrustCapacity,
    PlatformFinancialInclusion,
    AlgorithmicPricingRisk,
]

__all__ = [
    "PlatformMarketDominance",
    "NetworkEffectsIntensity",
    "GigWorkerVulnerability",
    "DataMonopolyRisk",
    "PlatformTaxErosion",
    "WinnerTakesAllDynamics",
    "PlatformLaborRegulationGap",
    "DigitalAntitrustCapacity",
    "PlatformFinancialInclusion",
    "AlgorithmicPricingRisk",
    "ALL_MODULES",
]
