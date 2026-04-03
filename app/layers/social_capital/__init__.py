from app.layers.social_capital.institutional_trust_index import InstitutionalTrustIndex
from app.layers.social_capital.civic_participation_index import CivicParticipationIndex
from app.layers.social_capital.social_cohesion_score import SocialCohesionScore
from app.layers.social_capital.norms_enforcement_quality import NormsEnforcementQuality
from app.layers.social_capital.community_network_density import CommunityNetworkDensity
from app.layers.social_capital.interpersonal_trust_proxy import InterpersonalTrustProxy
from app.layers.social_capital.cooperative_institution_index import CooperativeInstitutionIndex
from app.layers.social_capital.social_mobility_capital import SocialMobilityCapital
from app.layers.social_capital.volunteer_economy_size import VolunteerEconomySize
from app.layers.social_capital.bridging_bonding_ratio import BridgingBondingRatio

ALL_MODULES = [
    InstitutionalTrustIndex,
    CivicParticipationIndex,
    SocialCohesionScore,
    NormsEnforcementQuality,
    CommunityNetworkDensity,
    InterpersonalTrustProxy,
    CooperativeInstitutionIndex,
    SocialMobilityCapital,
    VolunteerEconomySize,
    BridgingBondingRatio,
]

__all__ = [
    "InstitutionalTrustIndex",
    "CivicParticipationIndex",
    "SocialCohesionScore",
    "NormsEnforcementQuality",
    "CommunityNetworkDensity",
    "InterpersonalTrustProxy",
    "CooperativeInstitutionIndex",
    "SocialMobilityCapital",
    "VolunteerEconomySize",
    "BridgingBondingRatio",
    "ALL_MODULES",
]
