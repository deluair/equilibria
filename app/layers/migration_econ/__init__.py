from app.layers.migration_econ.remittance_dependence import RemittanceDependence
from app.layers.migration_econ.brain_drain_risk import BrainDrainRisk
from app.layers.migration_econ.migration_push_factors import MigrationPushFactors
from app.layers.migration_econ.diaspora_investment import DiasporaInvestment
from app.layers.migration_econ.refugee_economic_impact import RefugeeEconomicImpact
from app.layers.migration_econ.labor_migration_flows import LaborMigrationFlows
from app.layers.migration_econ.migration_remittance_cycle import MigrationRemittanceCycle
from app.layers.migration_econ.internal_migration_pressure import InternalMigrationPressure
from app.layers.migration_econ.migration_governance import MigrationGovernance
from app.layers.migration_econ.return_migration_potential import ReturnMigrationPotential

ALL_MODULES = [
    RemittanceDependence,
    BrainDrainRisk,
    MigrationPushFactors,
    DiasporaInvestment,
    RefugeeEconomicImpact,
    LaborMigrationFlows,
    MigrationRemittanceCycle,
    InternalMigrationPressure,
    MigrationGovernance,
    ReturnMigrationPotential,
]

__all__ = [
    "RemittanceDependence",
    "BrainDrainRisk",
    "MigrationPushFactors",
    "DiasporaInvestment",
    "RefugeeEconomicImpact",
    "LaborMigrationFlows",
    "MigrationRemittanceCycle",
    "InternalMigrationPressure",
    "MigrationGovernance",
    "ReturnMigrationPotential",
    "ALL_MODULES",
]
