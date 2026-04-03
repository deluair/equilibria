from app.layers.cultural.social_capital import SocialCapital
from app.layers.cultural.trust_institutions import TrustInstitutions
from app.layers.cultural.diaspora_economics import DiasporaEconomics
from app.layers.cultural.cultural_consumption import CulturalConsumption
from app.layers.cultural.language_economics import LanguageEconomics
from app.layers.cultural.creative_industries import CreativeIndustries
from app.layers.cultural.norms_enforcement import NormsEnforcement
from app.layers.cultural.religion_economics import ReligionEconomics
from app.layers.cultural.indigenous_economics import IndigenousEconomics
from app.layers.cultural.cultural_distance import CulturalDistance

ALL_MODULES = [
    SocialCapital,
    TrustInstitutions,
    DiasporaEconomics,
    CulturalConsumption,
    LanguageEconomics,
    CreativeIndustries,
    NormsEnforcement,
    ReligionEconomics,
    IndigenousEconomics,
    CulturalDistance,
]

__all__ = [
    "SocialCapital",
    "TrustInstitutions",
    "DiasporaEconomics",
    "CulturalConsumption",
    "LanguageEconomics",
    "CreativeIndustries",
    "NormsEnforcement",
    "ReligionEconomics",
    "IndigenousEconomics",
    "CulturalDistance",
    "ALL_MODULES",
]
