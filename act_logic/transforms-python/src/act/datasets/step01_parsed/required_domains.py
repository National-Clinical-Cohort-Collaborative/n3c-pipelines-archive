from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from source_cdm_utils.parse import required_parse
from act.anchor import path
from act import local_schemas


def make_transform(domain):
    all_dict = local_schemas.complete_domain_schema_dict[domain]
    all_cols = list(all_dict.keys())

    required_dict = local_schemas.required_domain_schema_dict[domain]
    required_cols = list(required_dict.keys())

    checks = [
        Check(E.count().gt(0), 'Required dataset is not empty', 'FAIL')
    ]

    @transform(
        filename_input=Input(path.transform + "00 - unzipped/payload_filename"),
        payload_input=Input(path.transform + "00 - unzipped/unzipped_raw_data"),
        processed_output=Output(path.transform + "01 - parsed/required/" + domain, checks=checks),
        error_output=Output(path.transform + "01 - parsed/errors/" + domain)
    )
    def compute_function(filename_input, payload_input, processed_output, error_output):
        required_parse(filename_input, payload_input, domain, processed_output, error_output, all_cols, required_cols)

    return compute_function


domains = ["observation_fact", "patient_dimension", "visit_dimension"]

transforms = (make_transform(domain) for domain in domains)
