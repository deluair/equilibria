from app.layers.financial_regulation.capital_adequacy_regulation import CapitalAdequacyRegulation
from app.layers.financial_regulation.banking_supervision_quality import BankingSupervisionQuality
from app.layers.financial_regulation.regulatory_compliance_cost import RegulatoryComplianceCost
from app.layers.financial_regulation.consumer_protection_index import ConsumerProtectionIndex
from app.layers.financial_regulation.fintech_regulatory_gap import FintechRegulatoryGap
from app.layers.financial_regulation.systemic_risk_oversight import SystemicRiskOversight
from app.layers.financial_regulation.deposit_insurance_coverage import DepositInsuranceCoverage
from app.layers.financial_regulation.aml_cft_compliance import AmlCftCompliance
from app.layers.financial_regulation.regulatory_arbitrage_risk import RegulatoryArbitrageRisk
from app.layers.financial_regulation.macroprudential_policy_index import MacroprudentialPolicyIndex

ALL_MODULES = [
    CapitalAdequacyRegulation,
    BankingSupervisionQuality,
    RegulatoryComplianceCost,
    ConsumerProtectionIndex,
    FintechRegulatoryGap,
    SystemicRiskOversight,
    DepositInsuranceCoverage,
    AmlCftCompliance,
    RegulatoryArbitrageRisk,
    MacroprudentialPolicyIndex,
]

__all__ = [
    "CapitalAdequacyRegulation",
    "BankingSupervisionQuality",
    "RegulatoryComplianceCost",
    "ConsumerProtectionIndex",
    "FintechRegulatoryGap",
    "SystemicRiskOversight",
    "DepositInsuranceCoverage",
    "AmlCftCompliance",
    "RegulatoryArbitrageRisk",
    "MacroprudentialPolicyIndex",
    "ALL_MODULES",
]
