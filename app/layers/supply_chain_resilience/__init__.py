from app.layers.supply_chain_resilience.import_concentration_risk import ImportConcentrationRisk
from app.layers.supply_chain_resilience.logistics_performance_gap import LogisticsPerformanceGap
from app.layers.supply_chain_resilience.supply_chain_diversification import SupplyChainDiversification
from app.layers.supply_chain_resilience.critical_input_dependence import CriticalInputDependence
from app.layers.supply_chain_resilience.port_efficiency_index import PortEfficiencyIndex
from app.layers.supply_chain_resilience.inventory_buffer_adequacy import InventoryBufferAdequacy
from app.layers.supply_chain_resilience.nearshoring_friendliness import NearshoringFriendliness
from app.layers.supply_chain_resilience.supply_disruption_history import SupplyDisruptionHistory
from app.layers.supply_chain_resilience.strategic_stockpile_index import StrategicStockpileIndex
from app.layers.supply_chain_resilience.reshoring_capacity import ReshoringCapacity

ALL_MODULES = [
    ImportConcentrationRisk,
    LogisticsPerformanceGap,
    SupplyChainDiversification,
    CriticalInputDependence,
    PortEfficiencyIndex,
    InventoryBufferAdequacy,
    NearshoringFriendliness,
    SupplyDisruptionHistory,
    StrategicStockpileIndex,
    ReshoringCapacity,
]

__all__ = [
    "ImportConcentrationRisk",
    "LogisticsPerformanceGap",
    "SupplyChainDiversification",
    "CriticalInputDependence",
    "PortEfficiencyIndex",
    "InventoryBufferAdequacy",
    "NearshoringFriendliness",
    "SupplyDisruptionHistory",
    "StrategicStockpileIndex",
    "ReshoringCapacity",
    "ALL_MODULES",
]
