from transforms.api import transform, Input, Output, Check, configure
from transforms import expectations as E
from source_cdm_utils.parse import required_parse
from pcornet.anchor import path
from pcornet import local_schemas


def make_transform(domain):
    schema_dict = local_schemas.complete_domain_schema_dict[domain]
    schema_cols = list(schema_dict.keys())

    checks = [
        Check(E.count().gt(0), 'Required dataset is not empty', 'FAIL')
    ]

    @configure(profile=['DRIVER_MEMORY_LARGE', 'NUM_EXECUTORS_64', 'EXECUTOR_MEMORY_LARGE'])
    @transform(
        filename_input=Input(path.transform + "00 - unzipped/payload_filename"),
        payload_input=Input(path.transform + "00 - unzipped/unzipped_raw_data"),
        processed_output=Output(path.transform + "01 - parsed/required/" + domain, checks=checks),
        error_output=Output(path.transform + "01 - parsed/errors/" + domain)
    )
    def compute_function(filename_input, payload_input, processed_output, error_output):
        required_parse(filename_input, payload_input, domain, processed_output, error_output, schema_cols, schema_cols)

    return compute_function


domains = [
    "condition",
    "death",
    "demographic",
    "diagnosis",
    "encounter",
    "lab_result_cm",
    "med_admin",
    "prescribing",
    "procedures",
    "vital",
]

transforms = (make_transform(domain) for domain in domains)
