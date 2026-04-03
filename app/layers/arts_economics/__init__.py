from app.layers.arts_economics.creative_industries_gdp import CreativeIndustriesGDP
from app.layers.arts_economics.cultural_heritage_tourism_value import CulturalHeritageTourismValue
from app.layers.arts_economics.arts_employment_share import ArtsEmploymentShare
from app.layers.arts_economics.copyright_industry_value import CopyrightIndustryValue
from app.layers.arts_economics.cultural_exports_competitiveness import CulturalExportsCompetitiveness
from app.layers.arts_economics.arts_public_funding_adequacy import ArtsPublicFundingAdequacy
from app.layers.arts_economics.arts_market_concentration import ArtsMarketConcentration
from app.layers.arts_economics.creative_economy_innovation import CreativeEconomyInnovation
from app.layers.arts_economics.cultural_consumption_inequality import CulturalConsumptionInequality
from app.layers.arts_economics.music_film_industry_economics import MusicFilmIndustryEconomics

ALL_MODULES = [
    CreativeIndustriesGDP,
    CulturalHeritageTourismValue,
    ArtsEmploymentShare,
    CopyrightIndustryValue,
    CulturalExportsCompetitiveness,
    ArtsPublicFundingAdequacy,
    ArtsMarketConcentration,
    CreativeEconomyInnovation,
    CulturalConsumptionInequality,
    MusicFilmIndustryEconomics,
]

__all__ = [
    "CreativeIndustriesGDP",
    "CulturalHeritageTourismValue",
    "ArtsEmploymentShare",
    "CopyrightIndustryValue",
    "CulturalExportsCompetitiveness",
    "ArtsPublicFundingAdequacy",
    "ArtsMarketConcentration",
    "CreativeEconomyInnovation",
    "CulturalConsumptionInequality",
    "MusicFilmIndustryEconomics",
    "ALL_MODULES",
]
