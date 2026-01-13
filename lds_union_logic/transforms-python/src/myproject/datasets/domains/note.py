from transforms.api import transform_df, Output, Check
from transforms import expectations as E
from myproject.datasets.utils import get_inputs, perform_union

domain = 'note'
id_col = 'note_id'
inputs = get_inputs(domain, include_omop=True)  # , include_pcornet=True)
#  include_trinetx=False, include_act=False, include_pedsnet=False)


@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_{}".format(domain),
        checks=Check(E.primary_key(id_col), 'Valid primary key', on_error='FAIL')
    ),
    **inputs
)
def my_compute_function(ctx, **inputs):
    for df_site_name in inputs:
        if "note_class_type_id" in inputs[df_site_name].columns:
            inputs[df_site_name] = inputs[df_site_name].withColumnRenamed("note_class_type_id", "note_class_concept_id")

    df_out = perform_union(ctx, domain, **inputs)
    # df_out = df_out.repartition(10)
    return df_out
