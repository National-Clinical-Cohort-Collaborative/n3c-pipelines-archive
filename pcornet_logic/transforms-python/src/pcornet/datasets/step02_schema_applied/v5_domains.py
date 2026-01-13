from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from source_cdm_utils import schema
from pcornet import local_schemas
from pcornet.utils import apply_schema
from pcornet.anchor import path


def make_transform(domain, domain_v5, v5_column, pkey, folder):

    # Get complete PCORnet schema
    schema_dict = local_schemas.complete_domain_schema_dict[domain]
    schema_dict_v5 = local_schemas.complete_domain_schema_dict[domain_v5]

    # Get required PCORnet columns -- handle PCORnet 5.0 and 6.0 schemas
    required_dict = local_schemas.required_domain_schema_dict[domain]
    required_dict_v5 = local_schemas.required_domain_schema_dict[domain_v5]

    # Check that required columns are present, but they will all be Strings
    schema_strings = schema.schema_dict_all_string_type(required_dict)
    schema_strings_v5 = schema.schema_dict_all_string_type(required_dict_v5)

    schema_expectation = E.any(
        E.schema().contains(schema_strings),
        E.schema().contains(schema_strings_v5)
    )

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

        # Handle PCORnet 5.0 schema (containing obsclin_date and obsclin_time columns)
        lower_cols = [col.lower() for col in processed_df.columns]
        if v5_column in lower_cols:
            processed_df = apply_schema(processed_df, schema_dict_v5)
        else:
            processed_df = apply_schema(processed_df, schema_dict)

        processed.write_dataframe(processed_df)

    return compute_function


domains = [
    ("obs_clin", "obs_clin_5.0", "obsclin_date", "obsclinid", "optional"),
    ("obs_gen", "obs_gen_5.0", "obsgen_date", "obsgenid", "optional")
]

transforms = (make_transform(*domain) for domain in domains)
