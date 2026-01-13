from transforms.api import transform, Input, Output
from source_cdm_utils.unzip import unzipLatest
from pcornet.anchor import path

# to do - also add QC script within step00_unzip to check for unzip latest is performed correctly
@transform(
    zip_file=Input(path.input_zip),
    unzipped=Output(path.transform + "00 - unzipped/unzipped_raw_data")
)
def unzip(zip_file, unzipped):
    regex = "(?i).*incoming/.*_pcornet_.*\\.zip"
    unzipLatest(zip_file, regex, unzipped)
