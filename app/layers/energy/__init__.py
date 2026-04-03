from app.layers.energy.electricity_market import ElectricityMarket
from app.layers.energy.energy_efficiency import EnergyEfficiency
from app.layers.energy.energy_security import EnergySecurity
from app.layers.energy.energy_transition import EnergyTransition
from app.layers.energy.fossil_subsidy import FossilSubsidy
from app.layers.energy.oil_market import OilMarket

ALL_MODULES = [
    OilMarket,
    EnergySecurity,
    ElectricityMarket,
    EnergyEfficiency,
    FossilSubsidy,
    EnergyTransition,
]

__all__ = [
    "OilMarket",
    "EnergySecurity",
    "ElectricityMarket",
    "EnergyEfficiency",
    "FossilSubsidy",
    "EnergyTransition",
    "ALL_MODULES",
]
