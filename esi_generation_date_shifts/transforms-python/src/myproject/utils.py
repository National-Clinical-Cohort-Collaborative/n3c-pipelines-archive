from pyspark.sql import functions as F, Window

all_domains = [
    ("condition_era", None),
    ("condition_occurrence", None),
    ("death", None),
    ("device_exposure", None),
    ("drug_era", None),
    ("drug_exposure", ['DYNAMIC_ALLOCATION_MAX_8', 'NUM_EXECUTORS_8', 'DRIVER_MEMORY_MEDIUM', 'EXECUTOR_MEMORY_LARGE']),
    ("measurement", ['DYNAMIC_ALLOCATION_MAX_8', 'NUM_EXECUTORS_8', 'DRIVER_MEMORY_MEDIUM', 'EXECUTOR_MEMORY_LARGE']),
    ("observation", None),
    ("observation_period", None),
    ("procedure_occurrence", None),
    ("visit_occurrence", None),
    ("person", None),
    ("location", None),
    ("measurements_to_macrovisits", ['DYNAMIC_ALLOCATION_MAX_8', 'NUM_EXECUTORS_8', 'DRIVER_MEMORY_MEDIUM', 
    'EXECUTOR_MEMORY_LARGE']),
    ("procedures_to_macrovisits", None),
    ("conditions_to_macrovisit", ['DYNAMIC_ALLOCATION_MAX_8', 'NUM_EXECUTORS_8', 'DRIVER_MEMORY_MEDIUM', 'EXECUTOR_MEMORY_LARGE']),
    ("payer_plan_period", None),
    ("provider", None),
    ("care_site", None),
    ("microvisits_to_macrovisits", None),
    ("control_map", None),
    ("note", None),
    ("note_nlp", None),
    ("visit_detail", ['DYNAMIC_ALLOCATION_MAX_8', 'NUM_EXECUTORS_8', 'DRIVER_MEMORY_MEDIUM', 'EXECUTOR_MEMORY_LARGE']),
    ("manifest_harmonized", None),
]

all_id_cols = [
    "condition_era_id",
    "condition_occurrence_id",
    "device_exposure_id",
    "drug_era_id",
    "drug_exposure_id",
    "location_id",
    "measurement_id",
    "note_id",
    "note_nlp_id",
    "observation_id",
    "observation_period_id",
    "person_id",
    "procedure_occurrence_id",
    "visit_occurrence_id",
    "visit_detail_id",
    "control_map_id",
    "data_partner_id",
    "global_person_id",
    "macrovisit_id"

]

# Define a dictionary mapping each domain to its primary key columns
domain_primary_keys = {
    "condition_era": ["condition_era_id"],
    "condition_occurrence": ["condition_occurrence_id"],
    "device_exposure": ["device_exposure_id"],
    "drug_era": ["drug_era_id"],
    "drug_exposure": ["drug_exposure_id"],
    "measurement": ["measurement_id"],
    "observation": ["observation_id"],
    "observation_period": ["observation_period_id"],
    "procedure_occurrence": ["procedure_occurrence_id"],
    "visit_occurrence": ["visit_occurrence_id"],
    "person": ["person_id"],
    "location": ["location_id"],
    "payer_plan_period": ["payer_plan_period_id"],
    "provider": ["provider_id"],
    "care_site": ["care_site_id"],
    "microvisits_to_macrovisits": ["visit_occurrence_id"],
    "control_map": ["control_map_id"],
    "note": ["note_id"],
    "note_nlp": ["note_nlp_id"],
    "visit_detail": ["visit_detail_id"],
    #"manifest_harmonized": ["data_partner_id", "cdm_name"]
}


def resolve_collisions_and_create_id_column(df, id_col):
    """
    Returns a new column with collision bits added to id_col, 
    resolving duplicate hashed IDs in a distributed way.
    """
    # Window to partition by id_col and order by a unique column (or monotonically_increasing_id)
    w = Window.partitionBy(id_col).orderBy(F.monotonically_increasing_id())

    # Add collision bits: row_number - 1 within each id_col group
    df = df.withColumn(
        "collision_bits",
        F.row_number().over(w) - 1
    )

    # Combine id_col and collision_bits into a new_id
    new_id_col = (F.shiftLeft(F.col(id_col), 3) + F.col("collision_bits")).alias("new_id")

    return new_id_col