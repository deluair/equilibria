from app.layers.bioeconomy.biobased_industry_share import BiobsedIndustryShare
from app.layers.bioeconomy.agricultural_biotech_adoption import AgriculturalBiotechAdoption
from app.layers.bioeconomy.genetic_resource_value import GeneticResourceValue
from app.layers.bioeconomy.biopharmaceutical_market import BiopharmaceuticalMarket
from app.layers.bioeconomy.synthetic_biology_economics import SyntheticBiologyEconomics
from app.layers.bioeconomy.bioeconomy_employment import BioeconomyEmployment
from app.layers.bioeconomy.forest_bioeconomy_value import ForestBioeconomyValue
from app.layers.bioeconomy.marine_bioeconomy import MarineBioeconomy
from app.layers.bioeconomy.bioenergy_transition import BioenergyTransition
from app.layers.bioeconomy.bioeconomy_innovation_capacity import BioeconomyInnovationCapacity

ALL_MODULES = [
    BiobsedIndustryShare,
    AgriculturalBiotechAdoption,
    GeneticResourceValue,
    BiopharmaceuticalMarket,
    SyntheticBiologyEconomics,
    BioeconomyEmployment,
    ForestBioeconomyValue,
    MarineBioeconomy,
    BioenergyTransition,
    BioeconomyInnovationCapacity,
]

__all__ = [
    "BiobsedIndustryShare",
    "AgriculturalBiotechAdoption",
    "GeneticResourceValue",
    "BiopharmaceuticalMarket",
    "SyntheticBiologyEconomics",
    "BioeconomyEmployment",
    "ForestBioeconomyValue",
    "MarineBioeconomy",
    "BioenergyTransition",
    "BioeconomyInnovationCapacity",
    "ALL_MODULES",
]
