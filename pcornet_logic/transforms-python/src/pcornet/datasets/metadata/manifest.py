from transforms.api import transform_df, Input, Output, Check
from source_cdm_utils import manifest
from pcornet import local_schemas
from pcornet.anchor import path
from transforms import expectations as E

checks = [
    Check(E.count().lt(2), 'Manifest has less than two rows', 'FAIL')
]


@transform_df(
    Output(path.metadata + "manifest"),
    manifest_df=Input(path.transform + "01 - parsed/metadata/manifest", checks=checks),
    site_id_df=Input(path.site_id),
    data_partner_ids=Input(path.all_ids),
    omop_vocab=Input(path.vocab),
    control_map=Input(path.metadata + "control_map")
)
def my_compute_function(ctx, manifest_df, site_id_df, data_partner_ids, omop_vocab, control_map):
    schema_dict = local_schemas.manifest_schema
    return manifest.manifest(ctx, schema_dict, manifest_df, site_id_df, data_partner_ids, omop_vocab, control_map)
