import pyspark.sql.functions as F

release_branches = [
    "master"
]

# Domains to release
DOMAINS = [
    "care_site",
    "condition_era",
    "condition_occurrence",
    "control_map",
    "death",
    "device_exposure",
    "drug_era",
    "drug_exposure",
    "location",
    "measurement",
    "note",
    "note_nlp",
    "observation",
    "observation_period",
    "payer_plan_period",
    "person",
    "procedure_occurrence",
    "provider",
    "visit_detail",
    "visit_occurrence",
]

# Non-domain datasets will be released
DATASETS_TO_RELEASE = [
    'microvisits_to_macrovisits',
    'procedures_to_macrovisits',
    'conditions_to_macrovisits',
    'measurements_to_macrovisits',
    'manifest_safe_harbor'
]

# Non-domain datasets to stage as a group
# manifest_safe_harbor is not here, as it is processed separately in it's own transform
DATASETS_TO_STAGE = [
    'microvisits_to_macrovisits',
    'procedures_to_macrovisits',
    'conditions_to_macrovisits',
    'measurements_to_macrovisits'
]


def release_by_data_partner_id(df, manifest):
    manifest = manifest.filter(manifest["released"] == True)
    released_ids = [row.data_partner_id for row in manifest.select("data_partner_id").distinct().collect()]
    df = df.filter(df.data_partner_id.isin(released_ids))
    return df


def lower_case_cols(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    return df


def suppress_value_as_string(df):
    # If value_as_string has no valid value_as_concept_id, remove the text string
    # to remove possibility of PII leaking through this field
    df = df.withColumn(
        "value_as_string", F.when(F.col("value_as_concept_id") == 0, F.lit(None)).otherwise(F.col("value_as_string"))
    )
    return df


domain_partitions = {
    "care_site": 10,
    "condition_era": 20,
    "condition_occurrence": 100,
    "control_map": 10,
    "death": 10,
    "device_exposure": 10,
    "drug_era": 20,
    "drug_exposure": 200,
    "location": 10,
    "measurement": 500,
    "note": 20,
    "note_nlp": 100,
    "observation": 50,
    "observation_period": 10,
    "payer_plan_period": 10,
    "person": 10,
    "procedure_occurrence": 80,
    "provider": 10,
    "visit_occurrence": 25,
    "visit_detail": 20
}

dataset_partitions = {
    "measurements_to_macrovisits": 500,
    "conditions_to_macrovisits": 100,
    "procedures_to_macrovisits": 80
}
