from app.layers.public.education_economics import EducationEconomics
from app.layers.public.fiscal_federalism import FiscalFederalism
from app.layers.public.infrastructure import InfrastructureEconomics
from app.layers.public.public_goods import PublicGoods
from app.layers.public.social_protection import SocialProtection
from app.layers.public.tax_incidence import TaxIncidence

ALL_MODULES = [
    TaxIncidence,
    PublicGoods,
    FiscalFederalism,
    SocialProtection,
    EducationEconomics,
    InfrastructureEconomics,
]

__all__ = [
    "TaxIncidence",
    "PublicGoods",
    "FiscalFederalism",
    "SocialProtection",
    "EducationEconomics",
    "InfrastructureEconomics",
    "ALL_MODULES",
]
