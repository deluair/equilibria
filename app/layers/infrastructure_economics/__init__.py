from app.layers.infrastructure_economics.connectivity_index import ConnectivityIndex
from app.layers.infrastructure_economics.digital_infrastructure_score import DigitalInfrastructureScore
from app.layers.infrastructure_economics.electricity_access_gap import ElectricityAccessGap
from app.layers.infrastructure_economics.infrastructure_investment_gap import InfrastructureInvestmentGap
from app.layers.infrastructure_economics.infrastructure_maintenance_deficit import InfrastructureMaintenanceDeficit
from app.layers.infrastructure_economics.infrastructure_private_finance import InfrastructurePrivateFinance
from app.layers.infrastructure_economics.infrastructure_returns import InfrastructureReturns
from app.layers.infrastructure_economics.transport_quality_index import TransportQualityIndex
from app.layers.infrastructure_economics.urban_infrastructure_stress import UrbanInfrastructureStress
from app.layers.infrastructure_economics.water_sanitation_access import WaterSanitationAccess

ALL_MODULES = [
    InfrastructureInvestmentGap,
    TransportQualityIndex,
    ElectricityAccessGap,
    DigitalInfrastructureScore,
    WaterSanitationAccess,
    InfrastructureReturns,
    InfrastructureMaintenanceDeficit,
    UrbanInfrastructureStress,
    InfrastructurePrivateFinance,
    ConnectivityIndex,
]

__all__ = [
    "InfrastructureInvestmentGap",
    "TransportQualityIndex",
    "ElectricityAccessGap",
    "DigitalInfrastructureScore",
    "WaterSanitationAccess",
    "InfrastructureReturns",
    "InfrastructureMaintenanceDeficit",
    "UrbanInfrastructureStress",
    "InfrastructurePrivateFinance",
    "ConnectivityIndex",
    "ALL_MODULES",
]
