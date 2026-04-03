from app.layers.public.decentralization import Decentralization
from app.layers.public.education_economics import EducationEconomics
from app.layers.public.fiscal_federalism import FiscalFederalism
from app.layers.public.government_effectiveness import GovernmentEffectiveness
from app.layers.public.infrastructure import InfrastructureEconomics
from app.layers.public.pension_reform import PensionReform
from app.layers.public.procurement import PublicProcurement
from app.layers.public.public_debt_burden import PublicDebtBurden
from app.layers.public.public_goods import PublicGoods
from app.layers.public.public_investment_quality import PublicInvestmentQuality
from app.layers.public.regulatory_impact import RegulatoryImpact
from app.layers.public.social_protection import SocialProtection
from app.layers.public.social_protection_coverage import SocialProtectionCoverage
from app.layers.public.tax_compliance import TaxCompliance
from app.layers.public.tax_incidence import TaxIncidence

ALL_MODULES = [
    TaxIncidence,
    PublicGoods,
    FiscalFederalism,
    SocialProtection,
    EducationEconomics,
    InfrastructureEconomics,
    PensionReform,
    Decentralization,
    PublicProcurement,
    RegulatoryImpact,
    SocialProtectionCoverage,
    PublicDebtBurden,
    TaxCompliance,
    GovernmentEffectiveness,
    PublicInvestmentQuality,
]

__all__ = [
    "TaxIncidence",
    "PublicGoods",
    "FiscalFederalism",
    "SocialProtection",
    "EducationEconomics",
    "InfrastructureEconomics",
    "PensionReform",
    "Decentralization",
    "PublicProcurement",
    "RegulatoryImpact",
    "SocialProtectionCoverage",
    "PublicDebtBurden",
    "TaxCompliance",
    "GovernmentEffectiveness",
    "PublicInvestmentQuality",
    "ALL_MODULES",
]
