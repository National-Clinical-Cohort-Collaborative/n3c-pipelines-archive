from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from source_cdm_utils import schema
from pcornet import local_schemas
from pcornet.utils import apply_schema
from pcornet.anchor import path


def make_transform(domain, pkey, folder):
    # Get complete PCORnet schema
    schema_dict = local_schemas.complete_domain_schema_dict[domain]
    required_dict = local_schemas.required_domain_schema_dict[domain]
    schema_strings = schema.schema_dict_all_string_type(required_dict)
    schema_expectation = E.schema().contains(schema_strings)

    input_checks = [
        Check(schema_expectation, "`" + domain + "` schema must contain required columns", on_error='FAIL')
    ]

    output_checks = []
    if pkey:
        output_checks.append(Check(E.primary_key(pkey), '`' + pkey + '` must be a valid primary key', on_error='FAIL'))

    @transform(
        my_input=Input(path.transform + '01 - parsed/' + folder + '/' + domain, checks=input_checks),
        processed=Output(path.transform + '02 - clean/' + domain, checks=output_checks),
    )
    def compute_function(my_input, processed):

        processed_df = my_input.dataframe()
        processed_df = apply_schema(processed_df, schema_dict)

        processed.write_dataframe(processed_df)

    return compute_function


domains = [
    ("condition", "conditionid", "required"),
    ("control_map", None, "optional"),
    ("death", None, "required"),
    ("death_cause", None, "optional"),
    ("demographic", "patid", "required"),
    ("diagnosis", "diagnosisid", "required"),
    ("dispensing", "dispensingid", "optional"),
    ("encounter", "encounterid", "required"),
    ("immunization", "immunizationid", "optional"),
    ("lab_result_cm", "lab_result_cm_id", "required"),
    ("lds_address_history", "addressid", "optional"),
    ("med_admin", "medadminid", "required"),
    ("note", "note_id", "cached"),
    ("note_nlp", "note_nlp_id", "cached"),
    # obs_clin
    # obs_gen
    ("prescribing", "prescribingid", "required"),
    ("pro_cm", "pro_cm_id", "optional"),
    ("procedures", "proceduresid", "required"),
    ("provider", "providerid", "optional"),
    ("vital", "vitalid", "required")
]

transforms = (make_transform(*domain) for domain in domains)
