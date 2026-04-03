from app.layers.energy_security.energy_import_dependence import EnergyImportDependence
from app.layers.energy_security.fossil_fuel_reserve_life import FossilFuelReserveLife
from app.layers.energy_security.renewable_energy_resilience import RenewableEnergyResilience
from app.layers.energy_security.grid_interdependence_risk import GridInterdependenceRisk
from app.layers.energy_security.energy_price_shock_transmission import EnergyPriceShockTransmission
from app.layers.energy_security.strategic_petroleum_reserves import StrategicPetroleumReserves
from app.layers.energy_security.energy_sanction_vulnerability import EnergySanctionVulnerability
from app.layers.energy_security.critical_mineral_concentration import CriticalMineralConcentration
from app.layers.energy_security.energy_transition_just_gap import EnergyTransitionJustGap
from app.layers.energy_security.power_grid_reliability import PowerGridReliability

ALL_MODULES = [
    EnergyImportDependence,
    FossilFuelReserveLife,
    RenewableEnergyResilience,
    GridInterdependenceRisk,
    EnergyPriceShockTransmission,
    StrategicPetroleumReserves,
    EnergySanctionVulnerability,
    CriticalMineralConcentration,
    EnergyTransitionJustGap,
    PowerGridReliability,
]

__all__ = [
    "EnergyImportDependence",
    "FossilFuelReserveLife",
    "RenewableEnergyResilience",
    "GridInterdependenceRisk",
    "EnergyPriceShockTransmission",
    "StrategicPetroleumReserves",
    "EnergySanctionVulnerability",
    "CriticalMineralConcentration",
    "EnergyTransitionJustGap",
    "PowerGridReliability",
    "ALL_MODULES",
]
