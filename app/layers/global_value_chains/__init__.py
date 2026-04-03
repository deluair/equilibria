from app.layers.global_value_chains.gvc_participation import GVCParticipation
from app.layers.global_value_chains.value_capture import ValueCapture
from app.layers.global_value_chains.backward_linkages import BackwardLinkages
from app.layers.global_value_chains.forward_linkages import ForwardLinkages
from app.layers.global_value_chains.gvc_upgrading import GVCUpgrading
from app.layers.global_value_chains.supply_chain_disruption_risk import SupplyChainDisruptionRisk
from app.layers.global_value_chains.reshoring_pressure import ReshoringPressure
from app.layers.global_value_chains.gvc_environmental_footprint import GVCEnvironmentalFootprint
from app.layers.global_value_chains.digital_gvc import DigitalGVC
from app.layers.global_value_chains.gvc_resilience import GVCResilience

ALL_MODULES = [
    GVCParticipation,
    ValueCapture,
    BackwardLinkages,
    ForwardLinkages,
    GVCUpgrading,
    SupplyChainDisruptionRisk,
    ReshoringPressure,
    GVCEnvironmentalFootprint,
    DigitalGVC,
    GVCResilience,
]

__all__ = [
    "GVCParticipation",
    "ValueCapture",
    "BackwardLinkages",
    "ForwardLinkages",
    "GVCUpgrading",
    "SupplyChainDisruptionRisk",
    "ReshoringPressure",
    "GVCEnvironmentalFootprint",
    "DigitalGVC",
    "GVCResilience",
    "ALL_MODULES",
]
