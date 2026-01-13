from transforms.api import transform_df, Output, Check, configure
from transforms import expectations as E
from myproject.datasets.utils import get_inputs, perform_union

domain = "provider"
id_col = "provider_id"
#inputs = get_inputs(domain, include_trinetx=False, include_act=False)
inputs = get_inputs(domain, include_act=False)



LINTER_APPLIED_PROFILES = [
    "KUBERNETES_NO_EXECUTORS",
    "DRIVER_MEMORY_MEDIUM",
]


@configure(profile=LINTER_APPLIED_PROFILES)
@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_{}".format(domain),
        checks=Check(E.primary_key(id_col), 'Valid primary key', on_error='FAIL')
    ),
    **inputs
)
def my_compute_function(ctx, **inputs):
    df_out = perform_union(ctx, domain, **inputs)
    # df_out = df_out.repartition(10)
    return df_out
