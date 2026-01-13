from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from source_cdm_utils import schema
from act import local_schemas
from act.utils import apply_schema
from act.anchor import path


def make_transform(domain, folder, pkeys):
    schema_dict = local_schemas.complete_domain_schema_dict[domain]
    required_dict = local_schemas.required_domain_schema_dict[domain]
    schema_strings = schema.schema_dict_all_string_type(required_dict)
    schema_expectation = E.schema().contains(schema_strings)

    input_checks = [
        Check(schema_expectation, "`" + domain + "` schema must contain required columns", on_error='FAIL'),
    ]

    output_checks = []
    if pkeys:
        output_checks.append(Check(E.primary_key(*pkeys), 'Must have valid cleaned primary key columns', on_error='FAIL'))

    @transform(
        my_input=Input(path.transform + '01 - parsed/' + folder + '/' + domain, checks=input_checks),
        processed=Output(path.transform + '02 - clean/' + domain, checks=output_checks),
    )
    def compute_function(my_input, processed):
        processed_df = my_input.dataframe()

        processed_df = apply_schema(processed_df, schema_dict)

        # Special case for 688
        if domain == "observation_fact":
            processed_df = processed_df.withColumn("modifier_cd", F.coalesce(F.col("modifier_cd"), F.lit("@")))
            # special case for site 789 build failure with null provider_id
            processed_df = processed_df.withColumn("provider_id", F.coalesce(F.col("provider_id"), F.lit("@")))

        processed.write_dataframe(processed_df)

    return compute_function


domains = [
    ("concept_dimension", "optional", None),
    ("control_map", "optional", None),
    ("note", "cached", ["note_id"]),
    ("note_nlp", "cached", ["note_nlp_id"]),
    ("observation_fact", "required", ['encounter_num', 'concept_cd', 'provider_id', 'start_date', 'patient_num', 'modifier_cd', 'instance_num']),
    ("patient_dimension", "required", ["patient_num"]),
    ("visit_dimension", "required", ["patient_num", "encounter_num"])
]

transforms = (make_transform(*domain) for domain in domains)
