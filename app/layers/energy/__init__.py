from app.layers.energy.carbon_capture import CarbonCapture
from app.layers.energy.electricity_market import ElectricityMarket
from app.layers.energy.electrification_gap import ElectrificationGap
from app.layers.energy.energy_efficiency import EnergyEfficiency
from app.layers.energy.energy_import_dependency import EnergyImportDependency
from app.layers.energy.energy_productivity import EnergyProductivity
from app.layers.energy.energy_security import EnergySecurity
from app.layers.energy.energy_storage import EnergyStorage
from app.layers.energy.energy_transition import EnergyTransition
from app.layers.energy.fossil_fuel_dependency import FossilFuelDependency
from app.layers.energy.fossil_subsidy import FossilSubsidy
from app.layers.energy.hydrogen_economy import HydrogenEconomy
from app.layers.energy.nuclear_economics import NuclearEconomics
from app.layers.energy.oil_market import OilMarket
from app.layers.energy.renewable_readiness import RenewableReadiness

ALL_MODULES = [
    OilMarket,
    EnergySecurity,
    ElectricityMarket,
    EnergyEfficiency,
    FossilSubsidy,
    EnergyTransition,
    HydrogenEconomy,
    NuclearEconomics,
    CarbonCapture,
    EnergyStorage,
    FossilFuelDependency,
    EnergyImportDependency,
    RenewableReadiness,
    EnergyProductivity,
    ElectrificationGap,
]

__all__ = [
    "OilMarket",
    "EnergySecurity",
    "ElectricityMarket",
    "EnergyEfficiency",
    "FossilSubsidy",
    "EnergyTransition",
    "HydrogenEconomy",
    "NuclearEconomics",
    "CarbonCapture",
    "EnergyStorage",
    "FossilFuelDependency",
    "EnergyImportDependency",
    "RenewableReadiness",
    "EnergyProductivity",
    "ElectrificationGap",
    "ALL_MODULES",
]
