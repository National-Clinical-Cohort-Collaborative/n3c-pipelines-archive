from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from source_cdm_utils.parse import metadata_parse
from act import local_schemas
from source_cdm_utils import schema
from act.anchor import path


def make_transform(filename):
    schema_dict = local_schemas.metadata_schemas[filename]
    schema_strings = schema.schema_dict_all_string_type(schema_dict)
    schema_cols = list(schema_dict.keys())

    checks = [
        Check(E.schema().contains(schema_strings), "Schema must contain required fields", "FAIL")
    ]

    @transform(
        payload_input=Input(path.transform + "00 - unzipped/unzipped_raw_data"),
        processed_output=Output(path.transform + "01 - parsed/metadata/" + filename, checks=checks),
        error_output=Output(path.transform + "01 - parsed/errors/" + filename)
    )
    def compute_function(payload_input, processed_output, error_output):
        metadata_parse(payload_input, filename, processed_output, error_output, schema_cols, schema_cols)

    return compute_function


filenames = ["act_standard2local_code_map", "data_counts", "manifest", "n3c_vocab_map"]

transforms = (make_transform(filename) for filename in filenames)
