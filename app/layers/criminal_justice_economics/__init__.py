from app.layers.criminal_justice_economics.crime_gdp_drag import CrimeGDPDrag
from app.layers.criminal_justice_economics.incarceration_economic_cost import IncarcerationEconomicCost
from app.layers.criminal_justice_economics.policing_expenditure_efficiency import PolicingExpenditureEfficiency
from app.layers.criminal_justice_economics.organized_crime_economic_penetration import OrganizedCrimeEconomicPenetration
from app.layers.criminal_justice_economics.drug_market_economics import DrugMarketEconomics
from app.layers.criminal_justice_economics.recidivism_economic_impact import RecidivismEconomicImpact
from app.layers.criminal_justice_economics.corruption_crime_nexus import CorruptionCrimeNexus
from app.layers.criminal_justice_economics.financial_crime_cost import FinancialCrimeCost
from app.layers.criminal_justice_economics.victim_compensation_economics import VictimCompensationEconomics
from app.layers.criminal_justice_economics.crime_deterrence_returns import CrimeDeterrenceReturns

ALL_MODULES = [
    CrimeGDPDrag,
    IncarcerationEconomicCost,
    PolicingExpenditureEfficiency,
    OrganizedCrimeEconomicPenetration,
    DrugMarketEconomics,
    RecidivismEconomicImpact,
    CorruptionCrimeNexus,
    FinancialCrimeCost,
    VictimCompensationEconomics,
    CrimeDeterrenceReturns,
]

__all__ = [
    "CrimeGDPDrag",
    "IncarcerationEconomicCost",
    "PolicingExpenditureEfficiency",
    "OrganizedCrimeEconomicPenetration",
    "DrugMarketEconomics",
    "RecidivismEconomicImpact",
    "CorruptionCrimeNexus",
    "FinancialCrimeCost",
    "VictimCompensationEconomics",
    "CrimeDeterrenceReturns",
    "ALL_MODULES",
]
