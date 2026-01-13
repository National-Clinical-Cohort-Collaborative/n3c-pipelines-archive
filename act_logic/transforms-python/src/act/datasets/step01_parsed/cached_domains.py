from transforms.api import transform, Input, Output, configure
from source_cdm_utils.parse import cached_parse
from act.anchor import path
from act import local_schemas


def make_transform(domain):
    all_dict = local_schemas.complete_domain_schema_dict[domain]
    all_cols = list(all_dict.keys())

    required_dict = local_schemas.required_domain_schema_dict[domain]
    required_cols = list(required_dict.keys())

    @configure(profile=['KUBERNETES_NO_EXECUTORS'])
    @transform(
        filename_input=Input(path.transform + "00 - unzipped/payload_filename"),
        payload_input=Input(path.transform + "00 - unzipped/unzipped_raw_data"),
        processed_output=Output(path.transform + "01 - parsed/cached/" + domain),
        error_output=Output(path.transform + "01 - parsed/errors/" + domain)
    )
    def compute_function(filename_input, payload_input, processed_output, error_output):
        cached_parse(filename_input, payload_input, domain, processed_output, error_output, all_cols, required_cols)

    return compute_function


domains = ["note", "note_nlp"]

transforms = (make_transform(domain) for domain in domains)
