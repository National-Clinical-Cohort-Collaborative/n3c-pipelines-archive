MEDICARE_DATASETS_TO_RELEASE = [
    "condition_occurrence",
    "cms_person_id_to_n3c_person_id",
    "death",
    "care_site",
    "device_exposure",
    "health_system",
    "observation",
    "procedure_occurrence",
    "person",
    "visit_occurrence",
    "drug_exposure",
    "provider",
    "drug_era",
    "observation_period",
    "condition_era",
    "measurement"
]

MEDICAID_DATASETS_TO_RELEASE = [
    "condition_occurrence",
    "death",
    "cms_person_id_to_n3c_person_id",
    "care_site",
    "device_exposure",
    "health_system",
    "observation",
    "procedure_occurrence",
    "person",
    "visit_occurrence",
    "drug_exposure",
    "provider",
    "staged_cms_mapping_table",
    "drug_era",
    "observation_period",
    "condition_era",
    "measurement"
]

MEDICARE_CLAIMS_DATASETS_TO_RELEASE = [
    "member_benefit",
    "durable_medical_equipment",
    "home_health",
    "hospice",
    "inpatient",
    "professional_billed",
    "part_d",
    "skilled_nursing"
]


release_branches = [
    "master",
    "QA_Linkage"
]
