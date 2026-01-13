from transforms.api import transform, Input, Output
from source_cdm_utils.parse import optional_parse
from act.anchor import path
from act import local_schemas


def make_transform(domain):
    all_dict = local_schemas.complete_domain_schema_dict[domain]
    all_cols = list(all_dict.keys())

    required_dict = local_schemas.required_domain_schema_dict[domain]
    required_cols = list(required_dict.keys())

    @transform(
        filename_input=Input(path.transform + "00 - unzipped/payload_filename"),
        payload_input=Input(path.transform + "00 - unzipped/unzipped_raw_data"),
        processed_output=Output(path.transform + "01 - parsed/optional/" + domain),
        error_output=Output(path.transform + "01 - parsed/errors/" + domain)
    )
    def compute_function(filename_input, payload_input, processed_output, error_output):
        optional_parse(filename_input, payload_input, domain, processed_output, error_output, all_cols, required_cols)

    return compute_function


domains = ["concept_dimension", "control_map"]

transforms = (make_transform(domain) for domain in domains)
