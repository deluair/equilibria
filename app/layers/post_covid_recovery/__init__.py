from app.layers.post_covid_recovery.pandemic_output_loss import PandemicOutputLoss
from app.layers.post_covid_recovery.labor_market_scarring import LaborMarketScarring
from app.layers.post_covid_recovery.debt_overhang_burden import DebtOverhangBurden
from app.layers.post_covid_recovery.supply_chain_recovery import SupplyChainRecovery
from app.layers.post_covid_recovery.health_system_recovery import HealthSystemRecovery
from app.layers.post_covid_recovery.remote_work_permanence import RemoteWorkPermanence
from app.layers.post_covid_recovery.tourism_recovery_trajectory import TourismRecoveryTrajectory
from app.layers.post_covid_recovery.sme_viability import SMEViability
from app.layers.post_covid_recovery.education_learning_loss import EducationLearningLoss
from app.layers.post_covid_recovery.fiscal_space_exhaustion import FiscalSpaceExhaustion

ALL_MODULES = [
    PandemicOutputLoss,
    LaborMarketScarring,
    DebtOverhangBurden,
    SupplyChainRecovery,
    HealthSystemRecovery,
    RemoteWorkPermanence,
    TourismRecoveryTrajectory,
    SMEViability,
    EducationLearningLoss,
    FiscalSpaceExhaustion,
]

__all__ = [
    "PandemicOutputLoss",
    "LaborMarketScarring",
    "DebtOverhangBurden",
    "SupplyChainRecovery",
    "HealthSystemRecovery",
    "RemoteWorkPermanence",
    "TourismRecoveryTrajectory",
    "SMEViability",
    "EducationLearningLoss",
    "FiscalSpaceExhaustion",
    "ALL_MODULES",
]
