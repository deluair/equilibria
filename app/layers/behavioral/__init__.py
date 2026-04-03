from app.layers.behavioral.addiction_economics import AddictionEconomics
from app.layers.behavioral.availability_heuristic import AvailabilityHeuristic
from app.layers.behavioral.bounded_rationality import BoundedRationality
from app.layers.behavioral.charitable_giving import CharitableGiving
from app.layers.behavioral.confirmation_bias import ConfirmationBias
from app.layers.behavioral.framing_effects import FramingEffects
from app.layers.behavioral.herd_behavior import HerdBehavior
from app.layers.behavioral.inattention_economics import InattentionEconomics
from app.layers.behavioral.market_anomalies import MarketAnomalies
from app.layers.behavioral.mental_accounting import MentalAccounting
from app.layers.behavioral.nudge_evaluation import NudgeEvaluation
from app.layers.behavioral.overconfidence import Overconfidence
from app.layers.behavioral.overconfidence_investment import OverconfidenceInvestment
from app.layers.behavioral.privacy_valuation import PrivacyValuation
from app.layers.behavioral.prospect_theory import ProspectTheory
from app.layers.behavioral.regret_aversion import RegretAversion
from app.layers.behavioral.salience_theory import SalienceTheory
from app.layers.behavioral.social_preferences import SocialPreferences
from app.layers.behavioral.status_quo_bias import StatusQuoBias
from app.layers.behavioral.time_preference import TimePreference

ALL_MODULES = [
    ProspectTheory,
    NudgeEvaluation,
    TimePreference,
    BoundedRationality,
    SocialPreferences,
    MarketAnomalies,
    AddictionEconomics,
    PrivacyValuation,
    CharitableGiving,
    Overconfidence,
    HerdBehavior,
    StatusQuoBias,
    OverconfidenceInvestment,
    ConfirmationBias,
    AvailabilityHeuristic,
    MentalAccounting,
    InattentionEconomics,
    SalienceTheory,
    RegretAversion,
    FramingEffects,
]

__all__ = [
    "ProspectTheory",
    "NudgeEvaluation",
    "TimePreference",
    "BoundedRationality",
    "SocialPreferences",
    "MarketAnomalies",
    "AddictionEconomics",
    "PrivacyValuation",
    "CharitableGiving",
    "Overconfidence",
    "HerdBehavior",
    "StatusQuoBias",
    "OverconfidenceInvestment",
    "ConfirmationBias",
    "AvailabilityHeuristic",
    "MentalAccounting",
    "InattentionEconomics",
    "SalienceTheory",
    "RegretAversion",
    "FramingEffects",
    "ALL_MODULES",
]
