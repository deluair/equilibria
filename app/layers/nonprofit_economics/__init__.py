from app.layers.nonprofit_economics.charitable_giving_elasticity import CharitableGivingElasticity
from app.layers.nonprofit_economics.crowdfunding_economics import CrowdfundingEconomics
from app.layers.nonprofit_economics.impact_investment_return import ImpactInvestmentReturn
from app.layers.nonprofit_economics.ngo_efficiency import NGOEfficiency
from app.layers.nonprofit_economics.nonprofit_labor_market import NonprofitLaborMarket
from app.layers.nonprofit_economics.nonprofit_sector_size import NonprofitSectorSize
from app.layers.nonprofit_economics.nonprofit_tax_expenditures import NonprofitTaxExpenditures
from app.layers.nonprofit_economics.philanthropy_capital_allocation import PhilanthropyCapitalAllocation
from app.layers.nonprofit_economics.social_enterprise_viability import SocialEnterpriseViability
from app.layers.nonprofit_economics.volunteering_economic_value import VolunteeringEconomicValue

ALL_MODULES = [
    NonprofitSectorSize,
    PhilanthropyCapitalAllocation,
    CharitableGivingElasticity,
    SocialEnterpriseViability,
    NGOEfficiency,
    NonprofitLaborMarket,
    CrowdfundingEconomics,
    ImpactInvestmentReturn,
    VolunteeringEconomicValue,
    NonprofitTaxExpenditures,
]

__all__ = [
    "NonprofitSectorSize",
    "PhilanthropyCapitalAllocation",
    "CharitableGivingElasticity",
    "SocialEnterpriseViability",
    "NGOEfficiency",
    "NonprofitLaborMarket",
    "CrowdfundingEconomics",
    "ImpactInvestmentReturn",
    "VolunteeringEconomicValue",
    "NonprofitTaxExpenditures",
    "ALL_MODULES",
]
