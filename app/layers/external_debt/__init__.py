from app.layers.external_debt.external_debt_to_gdp import ExternalDebtToGdp
from app.layers.external_debt.debt_service_ratio import DebtServiceRatio
from app.layers.external_debt.short_term_debt_share import ShortTermDebtShare
from app.layers.external_debt.external_debt_composition import ExternalDebtComposition
from app.layers.external_debt.currency_mismatch_risk import CurrencyMismatchRisk
from app.layers.external_debt.rollover_risk_index import RolloverRiskIndex
from app.layers.external_debt.sovereign_debt_distress import SovereignDebtDistress
from app.layers.external_debt.debt_transparency_index import DebtTransparencyIndex
from app.layers.external_debt.creditor_concentration_risk import CreditorConcentrationRisk
from app.layers.external_debt.debt_restructuring_history import DebtRestructuringHistory

ALL_MODULES = [
    ExternalDebtToGdp,
    DebtServiceRatio,
    ShortTermDebtShare,
    ExternalDebtComposition,
    CurrencyMismatchRisk,
    RolloverRiskIndex,
    SovereignDebtDistress,
    DebtTransparencyIndex,
    CreditorConcentrationRisk,
    DebtRestructuringHistory,
]

__all__ = [
    "ExternalDebtToGdp",
    "DebtServiceRatio",
    "ShortTermDebtShare",
    "ExternalDebtComposition",
    "CurrencyMismatchRisk",
    "RolloverRiskIndex",
    "SovereignDebtDistress",
    "DebtTransparencyIndex",
    "CreditorConcentrationRisk",
    "DebtRestructuringHistory",
    "ALL_MODULES",
]
