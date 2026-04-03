"""Migration Integration layer (lMI) - 10 modules."""

from app.layers.migration_integration.immigrant_labor_integration import ImmigrantLaborIntegration
from app.layers.migration_integration.fiscal_cost_immigration import FiscalCostImmigration
from app.layers.migration_integration.immigrant_entrepreneurship import ImmigrantEntrepreneurship
from app.layers.migration_integration.language_skill_gap import LanguageSkillGap
from app.layers.migration_integration.housing_integration_stress import HousingIntegrationStress
from app.layers.migration_integration.xenophobia_economic_cost import XenophobiaEconomicCost
from app.layers.migration_integration.migration_human_capital import MigrationHumanCapital
from app.layers.migration_integration.remittance_multiplier import RemittanceMultiplier
from app.layers.migration_integration.diaspora_network_strength import DiasporaNetworkStrength
from app.layers.migration_integration.integration_policy_quality import IntegrationPolicyQuality

ALL_MODULES = [
    ImmigrantLaborIntegration,
    FiscalCostImmigration,
    ImmigrantEntrepreneurship,
    LanguageSkillGap,
    HousingIntegrationStress,
    XenophobiaEconomicCost,
    MigrationHumanCapital,
    RemittanceMultiplier,
    DiasporaNetworkStrength,
    IntegrationPolicyQuality,
]

__all__ = ["ALL_MODULES"]
