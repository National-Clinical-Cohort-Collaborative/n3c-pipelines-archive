from transforms.api import configure, transform_df, Output, Check
from transforms import expectations as E
from myproject.datasets.utils import get_inputs, perform_union

domain = 'drug_exposure'
id_col = "drug_exposure_id"
inputs = get_inputs(domain)


@configure(profile=['EXECUTOR_MEMORY_OVERHEAD_MEDIUM', 'NUM_EXECUTORS_8'])
@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_{}".format(domain),
        checks=Check(E.primary_key(id_col), 'Valid primary key', on_error='FAIL')
    ),
    **inputs
)
def my_compute_function(ctx, **inputs):
    df_out = perform_union(ctx, domain, **inputs)
    # df_out = df_out.repartition(500)
    return df_out
