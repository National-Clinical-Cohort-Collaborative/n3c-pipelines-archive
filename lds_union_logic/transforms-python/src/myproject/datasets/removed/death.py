from transforms.api import transform_df, Output, configure
from myproject.datasets.utils import get_removed_inputs, perform_removed_union

domain = 'death'
id_col = 'person_id'
inputs = get_removed_inputs(domain)


LINTER_APPLIED_PROFILES = [
    "KUBERNETES_NO_EXECUTORS",
    "DRIVER_MEMORY_MEDIUM",
]


@configure(profile=LINTER_APPLIED_PROFILES)
@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/removed/unioned_{}".format(domain),
    ),
    **inputs
)
def my_compute_function(ctx, **inputs):
    df_out = perform_removed_union(ctx, domain, **inputs)
    # df_out = df_out.repartition(10)
    return df_out
