from app.layers.health_technology.medical_innovation_index import MedicalInnovationIndex
from app.layers.health_technology.digital_health_adoption import DigitalHealthAdoption
from app.layers.health_technology.hta_capacity_index import HtaCapacityIndex
from app.layers.health_technology.telemedicine_readiness import TelemedicineReadiness
from app.layers.health_technology.medical_device_access import MedicalDeviceAccess
from app.layers.health_technology.pharmaceutical_innovation import PharmaceuticalInnovation
from app.layers.health_technology.health_ai_readiness import HealthAiReadiness
from app.layers.health_technology.clinical_trial_capacity import ClinicalTrialCapacity
from app.layers.health_technology.health_data_infrastructure import HealthDataInfrastructure
from app.layers.health_technology.cost_effectiveness_frontier import CostEffectivenessFrontier

ALL_MODULES = [
    MedicalInnovationIndex,
    DigitalHealthAdoption,
    HtaCapacityIndex,
    TelemedicineReadiness,
    MedicalDeviceAccess,
    PharmaceuticalInnovation,
    HealthAiReadiness,
    ClinicalTrialCapacity,
    HealthDataInfrastructure,
    CostEffectivenessFrontier,
]

__all__ = [
    "MedicalInnovationIndex",
    "DigitalHealthAdoption",
    "HtaCapacityIndex",
    "TelemedicineReadiness",
    "MedicalDeviceAccess",
    "PharmaceuticalInnovation",
    "HealthAiReadiness",
    "ClinicalTrialCapacity",
    "HealthDataInfrastructure",
    "CostEffectivenessFrontier",
    "ALL_MODULES",
]
