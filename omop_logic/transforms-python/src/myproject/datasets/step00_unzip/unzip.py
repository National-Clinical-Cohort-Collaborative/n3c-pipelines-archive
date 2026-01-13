from transforms.api import transform, Input, Output, configure
from source_cdm_utils.unzip import unzipLatest
from myproject.anchor import path


@configure(profile=['DRIVER_MEMORY_OVERHEAD_EXTRA_LARGE'])
@transform(
    zip_file=Input(path.input_zip),
    unzipped=Output(path.transform + "00 - unzipped/unzipped_raw_data"),
)
def unzip(zip_file, unzipped):
    regex = "(?i).*incoming/.*_OMOP_.*\\.zip"
    unzipLatest(zip_file, regex, unzipped)
