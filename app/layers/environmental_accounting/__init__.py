from app.layers.environmental_accounting.genuine_savings_rate import GenuineSavingsRate
from app.layers.environmental_accounting.natural_capital_depletion import NaturalCapitalDepletion
from app.layers.environmental_accounting.green_gdp_gap import GreenGdpGap
from app.layers.environmental_accounting.pollution_damage_cost import PollutionDamageCost
from app.layers.environmental_accounting.forest_depletion_rate import ForestDepletionRate
from app.layers.environmental_accounting.mineral_depletion_rate import MineralDepletionRate
from app.layers.environmental_accounting.energy_depletion_rate import EnergyDepletionRate
from app.layers.environmental_accounting.carbon_damage_estimate import CarbonDamageEstimate
from app.layers.environmental_accounting.ecological_overshoot import EcologicalOvershoot
from app.layers.environmental_accounting.seea_compliance_index import SeeaComplianceIndex

ALL_MODULES = [
    GenuineSavingsRate,
    NaturalCapitalDepletion,
    GreenGdpGap,
    PollutionDamageCost,
    ForestDepletionRate,
    MineralDepletionRate,
    EnergyDepletionRate,
    CarbonDamageEstimate,
    EcologicalOvershoot,
    SeeaComplianceIndex,
]

__all__ = [
    "GenuineSavingsRate",
    "NaturalCapitalDepletion",
    "GreenGdpGap",
    "PollutionDamageCost",
    "ForestDepletionRate",
    "MineralDepletionRate",
    "EnergyDepletionRate",
    "CarbonDamageEstimate",
    "EcologicalOvershoot",
    "SeeaComplianceIndex",
    "ALL_MODULES",
]
