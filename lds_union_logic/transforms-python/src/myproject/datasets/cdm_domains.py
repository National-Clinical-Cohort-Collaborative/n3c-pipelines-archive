core_domains = [
    "CONDITION_ERA",
    "CONDITION_OCCURRENCE",
    "DEATH",
    "DRUG_ERA",
    "DRUG_EXPOSURE",
    "LOCATION",
    "MEASUREMENT",
    "OBSERVATION",
    "OBSERVATION_PERIOD",
    "PERSON",
    "PROCEDURE_OCCURRENCE",
    "VISIT_OCCURRENCE"
]

# OMOP domains that have been mapped to by each source CDM
domains_by_cdm = {
    "trinetx":  [*core_domains, "DEVICE_EXPOSURE"],
    "omop":     [*core_domains, "CARE_SITE", "PROVIDER", "DOSE_ERA"],
    "act":      [*core_domains, "DEVICE_EXPOSURE"],
    "pcornet":  [*core_domains, "CARE_SITE", "DEVICE_EXPOSURE", "PAYER_PLAN_PERIOD", "PROVIDER"],
    "pedsnet":  [*core_domains, "CARE_SITE", "PROVIDER", "DOSE_ERA"],
}
