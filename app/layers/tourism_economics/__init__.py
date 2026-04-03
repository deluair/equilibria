from app.layers.tourism_economics.tourism_gdp_share import TourismGdpShare
from app.layers.tourism_economics.tourism_dependence_risk import TourismDependenceRisk
from app.layers.tourism_economics.tourist_arrival_trend import TouristArrivalTrend
from app.layers.tourism_economics.tourism_infrastructure_gap import TourismInfrastructureGap
from app.layers.tourism_economics.eco_tourism_sustainability import EcoTourismSustainability
from app.layers.tourism_economics.tourism_multiplier_effect import TourismMultiplierEffect
from app.layers.tourism_economics.visa_openness_index import VisaOpennessIndex
from app.layers.tourism_economics.tourism_seasonality_risk import TourismSeasonalityRisk
from app.layers.tourism_economics.cultural_heritage_value import CulturalHeritageValue
from app.layers.tourism_economics.tourism_employment_share import TourismEmploymentShare

ALL_MODULES = [
    TourismGdpShare,
    TourismDependenceRisk,
    TouristArrivalTrend,
    TourismInfrastructureGap,
    EcoTourismSustainability,
    TourismMultiplierEffect,
    VisaOpennessIndex,
    TourismSeasonalityRisk,
    CulturalHeritageValue,
    TourismEmploymentShare,
]

__all__ = [
    "TourismGdpShare",
    "TourismDependenceRisk",
    "TouristArrivalTrend",
    "TourismInfrastructureGap",
    "EcoTourismSustainability",
    "TourismMultiplierEffect",
    "VisaOpennessIndex",
    "TourismSeasonalityRisk",
    "CulturalHeritageValue",
    "TourismEmploymentShare",
    "ALL_MODULES",
]
