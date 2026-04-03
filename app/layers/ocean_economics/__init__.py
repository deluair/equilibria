"""L-OE Ocean Economics layer modules."""

from app.layers.ocean_economics.blue_carbon_value import BlueCarbonValue
from app.layers.ocean_economics.blue_economy_share import BlueEconomyShare
from app.layers.ocean_economics.coastal_vulnerability import CoastalVulnerability
from app.layers.ocean_economics.fisheries_economic_value import FisheriesEconomicValue
from app.layers.ocean_economics.marine_biodiversity_risk import MarineBiodiversityRisk
from app.layers.ocean_economics.maritime_trade_dependence import MaritimeTradeDepence as MaritimeTradeDependence
from app.layers.ocean_economics.ocean_pollution_cost import OceanPollutionCost
from app.layers.ocean_economics.port_connectivity_index import PortConnectivityIndex
from app.layers.ocean_economics.sea_level_economic_risk import SeaLevelEconomicRisk
from app.layers.ocean_economics.shipping_cost_burden import ShippingCostBurden

ALL_MODULES = [
    FisheriesEconomicValue,
    MaritimeTradeDependence,
    BlueCarbonValue,
    ShippingCostBurden,
    OceanPollutionCost,
    CoastalVulnerability,
    PortConnectivityIndex,
    MarineBiodiversityRisk,
    BlueEconomyShare,
    SeaLevelEconomicRisk,
]
