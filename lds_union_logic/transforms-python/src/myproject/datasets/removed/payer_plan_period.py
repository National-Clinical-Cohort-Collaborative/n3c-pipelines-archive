from transforms.api import transform_df, Output, Check, configure
from transforms import expectations as E
from myproject.datasets.utils import get_removed_inputs, perform_removed_union

domain = 'payer_plan_period'
id_col = 'payer_plan_period_id'
inputs = get_removed_inputs(domain, include_omop=False, include_pcornet=True)
# include_pedsnet=False, include_trinetx=False, include_act=False, )


LINTER_APPLIED_PROFILES = [
    "KUBERNETES_NO_EXECUTORS",
    "DRIVER_MEMORY_MEDIUM",
]


@configure(profile=LINTER_APPLIED_PROFILES)
@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/removed/unioned_{}".format(domain),
        checks=Check(E.primary_key(id_col), 'Valid primary key', on_error='FAIL')
    ),
    **inputs
)
def my_compute_function(ctx, **inputs):
    df_out = perform_removed_union(ctx, domain, **inputs)
    # df_out = df_out.repartition(10)
    return df_out
