from app.layers.digital_finance.financial_inclusion import FinancialInclusion
from app.layers.digital_finance.mobile_payment_adoption import MobilePaymentAdoption
from app.layers.digital_finance.digital_banking_depth import DigitalBankingDepth
from app.layers.digital_finance.fintech_regulatory_environment import FintechRegulatoryEnvironment
from app.layers.digital_finance.cbdc_readiness import CBDCReadiness
from app.layers.digital_finance.digital_divide import DigitalDivide
from app.layers.digital_finance.cybersecurity_risk import CybersecurityRisk
from app.layers.digital_finance.remittance_digitization import RemittanceDigitization
from app.layers.digital_finance.open_banking_index import OpenBankingIndex
from app.layers.digital_finance.crypto_vulnerability import CryptoVulnerability

ALL_MODULES = [
    FinancialInclusion,
    MobilePaymentAdoption,
    DigitalBankingDepth,
    FintechRegulatoryEnvironment,
    CBDCReadiness,
    DigitalDivide,
    CybersecurityRisk,
    RemittanceDigitization,
    OpenBankingIndex,
    CryptoVulnerability,
]

__all__ = [
    "FinancialInclusion",
    "MobilePaymentAdoption",
    "DigitalBankingDepth",
    "FintechRegulatoryEnvironment",
    "CBDCReadiness",
    "DigitalDivide",
    "CybersecurityRisk",
    "RemittanceDigitization",
    "OpenBankingIndex",
    "CryptoVulnerability",
    "ALL_MODULES",
]
